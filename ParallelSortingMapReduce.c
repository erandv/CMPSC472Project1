// ParallelSortingMapReduce.c
// Project: MapReduce-style Parallel Sorting
// This program sorts a big array using either threads or processes (Map + Reduce style).
// Map = each worker sorts its slice, Reduce = merge all sorted chunks.
// Build: gcc -O2 -pthread ParallelSortingMapReduce.c -o ParallelSortingMapReduce
// Run:   ./ParallelSortingMapReduce threads 8 131072
//        ./ParallelSortingMapReduce procs   8 131072

#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <stdbool.h>
#include <time.h>
#include <unistd.h>
#include <pthread.h>
#include <sys/wait.h>
#include <string.h>
#include <errno.h>

// write and read helpers for process pipes
static void write_all(int fd, const void* buf, size_t n) {
    const char* p = (const char*)buf;
    while (n) {
        ssize_t w = write(fd, p, n);
        if (w < 0) { perror("write"); exit(1); }
        p += (size_t)w;
        n -= (size_t)w;
    }
}
static void read_all(int fd, void* buf, size_t n) {
    char* p = (char*)buf;
    while (n) {
        ssize_t r = read(fd, p, n);
        if (r <= 0) { perror("read"); exit(1); }
        p += (size_t)r;
        n -= (size_t)r;
    }
}

// simple timer
static double now_s(void) {
    struct timespec t;
    clock_gettime(CLOCK_MONOTONIC, &t);
    return t.tv_sec + t.tv_nsec * 1e-9;
}

// fills the array with random numbers 
static void fill_rand(int32_t* a, size_t n, unsigned seed) {
    if (seed)
        srandom(seed);
    else
        srandom((unsigned)time(NULL));

    for (size_t i = 0; i < n; i++) {
        a[i] = (int32_t)random();
    }
}

// compare function for qsort
static int cmp_i32(const void* pa, const void* pb) {
    int32_t a = *(const int32_t*)pa;
    int32_t b = *(const int32_t*)pb;
    if (a < b) return -1;
    if (a > b) return 1;
    return 0;
}

// quick check to confirm array is sorted
static bool is_sorted(const int32_t* a, size_t n) {
    for (size_t i = 1; i < n; i++) {
        if (a[i - 1] > a[i]) return false;
    }
    return true;
}

// merges two sorted runs into one
static void merge_two(const int32_t* src,
    size_t l1, size_t r1,
    size_t l2, size_t r2,
    int32_t* dst, size_t out_lo) {
    size_t i = l1;
    size_t j = l2;
    size_t k = out_lo;

    while (i < r1 || j < r2) {
        bool take_left = (j >= r2) || (i < r1 && src[i] <= src[j]);
        dst[k] = take_left ? src[i] : src[j];
        if (take_left) i++;
        else j++;
        k++;
    }
}

// pairwise merging (used by reducer)
static void merge_runs(int32_t* a, size_t* lo, size_t* hi, int W) {
    size_t total = hi[W - 1];
    int32_t* tmp = (int32_t*)malloc(total * sizeof(int32_t));
    int active = W;

    while (active > 1) {
        int out = 0;
        for (int i = 0; i < active; i += 2) {
            int j = i + 1;
            size_t l1 = lo[i];
            size_t r1 = hi[i];
            size_t l2 = (j < active) ? lo[j] : l1;
            size_t r2 = (j < active) ? hi[j] : l1;
            merge_two(a, l1, r1, l2, r2, tmp, l1);
            for (size_t p = l1; p < r2; p++) a[p] = tmp[p];
            lo[out] = l1;
            hi[out] = r2;
            out++;
        }
        active = (active + 1) / 2;
    }
    free(tmp);
}

/* ---------------- THREADS VERSION ---------------- */

typedef struct {
    int32_t* a;
    size_t   l, r;
} Slice;

// thread worker: sorts its slice
static void* th_sort(void* arg) {
    Slice* s = (Slice*)arg;
    qsort(s->a + s->l, s->r - s->l, sizeof(int32_t), cmp_i32);
    return NULL;
}

// runs sorting using threads (Map = parallel sort, Reduce = merge)
static void run_threads(int32_t* a, size_t N, int W, double* tmap, double* tred) {
    double t0 = now_s();

    pthread_t th[8];
    Slice     sl[8];
    size_t    lo[8];
    size_t    hi[8];
    size_t step = (N + W - 1) / W;

    // create threads for map phase
    for (int w = 0; w < W; w++) {
        size_t l = (size_t)w * step;
        size_t r = (l + step > N) ? N : (l + step);
        sl[w].a = a;
        sl[w].l = l;
        sl[w].r = r;
        lo[w] = l;
        hi[w] = r;
        pthread_create(&th[w], NULL, th_sort, &sl[w]);
    }

    // wait for all threads
    for (int w = 0; w < W; w++) {
        pthread_join(th[w], NULL);
    }

    double t1 = now_s();

    // reduce phase = merge sorted chunks
    merge_runs(a, lo, hi, W);

    double t2 = now_s();
    *tmap = t1 - t0;
    *tred = t2 - t1;
}

/* ---------------- PROCESSES VERSION ---------------- */

typedef struct { int rd; int wr; } PipePair;

// runs sorting using processes (IPC = pipes)
static void run_procs(int32_t* a, size_t N, int W, double* tmap, double* tred) {
    double t0 = now_s();

    PipePair p[8];
    for (int i = 0; i < W; i++) {
        int fd[2];
        if (pipe(fd) != 0) { perror("pipe"); exit(1); }
        p[i].rd = fd[0];
        p[i].wr = fd[1];
    }

    pid_t pid[8];
    size_t step = (N + W - 1) / W;

    // fork W workers (each sorts its own chunk)
    for (int w = 0; w < W; w++) {
        size_t l = (size_t)w * step;
        size_t r = (l + step > N) ? N : (l + step);
        pid[w] = fork();
        if (pid[w] == 0) {
            for (int k = 0; k < W; k++) {
                if (k != w) {
                    close(p[k].rd);
                    close(p[k].wr);
                }
            }
            qsort(a + l, r - l, sizeof(int32_t), cmp_i32);
            size_t cnt = r - l;
            write_all(p[w].wr, &cnt, sizeof(cnt));
            write_all(p[w].wr, a + l, cnt * sizeof(int32_t));
            _exit(0);
        }
        close(p[w].wr);
    }

    // reducer: read all sorted chunks from pipes
    size_t lo[8];
    size_t hi[8];
    size_t off = 0;

    for (int w = 0; w < W; w++) {
        size_t cnt = 0;
        read_all(p[w].rd, &cnt, sizeof(cnt));
        read_all(p[w].rd, a + off, cnt * sizeof(int32_t));
        close(p[w].rd);
        lo[w] = off;
        hi[w] = off + cnt;
        off = hi[w];
    }

    for (int w = 0; w < W; w++) {
        int st = 0;
        waitpid(pid[w], &st, 0);
    }

    double t1 = now_s();

    // reduce: merge all sorted runs
    merge_runs(a, lo, hi, W);

    double t2 = now_s();
    *tmap = t1 - t0;
    *tred = t2 - t1;
}

/* ---------------- MAIN ---------------- */

int main(int argc, char** argv) {
    if (argc < 4) {
        fprintf(stderr, "Usage: %s <threads|procs> <workers> <N>\n", argv[0]);
        return 1;
    }

    bool threads = (strcmp(argv[1], "threads") == 0);
    int  W = atoi(argv[2]);
    size_t N = (size_t)atol(argv[3]);

    if (!(W == 1 || W == 2 || W == 4 || W == 8)) {
        fprintf(stderr, "workers must be 1, 2, 4, or 8\n");
        return 1;
    }

    // allocate and fill data
    int32_t* a = (int32_t*)malloc(N * sizeof(int32_t));
    if (!a) { perror("malloc"); return 1; }
    fill_rand(a, N, 42u);

    double tmap = 0.0, tred = 0.0;
    double t0 = now_s();

    if (threads)
        run_threads(a, N, W, &tmap, &tred);
    else
        run_procs(a, N, W, &tmap, &tred);

    double tall = now_s() - t0;

    // print results
    printf("Sorted? %s\n", is_sorted(a, N) ? "YES" : "NO");
    printf("Map    : %.6f s\n", tmap);
    printf("Reduce : %.6f s\n", tred);
    printf("Total  : %.6f s\n", tall);

    free(a);
    return 0;
}
