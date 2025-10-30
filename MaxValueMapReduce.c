// MaxValueMapReduce.c
// Project: MapReduce-style Max Value Aggregation
// This program finds the global max from a big array using threads or processes.
// It shows how to use parallelism, shared memory, and synchronization with a single shared value.
// Build: gcc -O2 -pthread MaxValueMapReduce.c -o MaxValueMapReduce
// Run:   ./MaxValueMapReduce threads 8 131072
//        ./MaxValueMapReduce procs   8 131072
//        ./MaxValueMapReduce threads 8 131072 nosync

#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <stdbool.h>
#include <limits.h>
#include <time.h>
#include <unistd.h>
#include <pthread.h>
#include <sys/wait.h>
#include <sys/mman.h>
#include <fcntl.h>
#include <semaphore.h>
#include <string.h>   // for strcmp()

// quick timer helper
static double now_s(void) {
    struct timespec t;
    clock_gettime(CLOCK_MONOTONIC, &t);
    return t.tv_sec + t.tv_nsec * 1e-9;
}

// fills the array with random numbers (using random() for simplicity)
static void fill_rand(int32_t* a, size_t n, unsigned seed) {
    if (seed)
        srandom(seed);
    else
        srandom((unsigned)time(NULL));

    for (size_t i = 0; i < n; i++) {
        a[i] = (int32_t)random();
    }
}

// serial version just for checking correctness
static int32_t serial_max(const int32_t* a, size_t n) {
    int32_t m = INT_MIN;
    for (size_t i = 0; i < n; i++) {
        if (a[i] > m) m = a[i];
    }
    return m;
}

/* ---------------- THREADS VERSION ---------------- */

typedef struct {
    int32_t* a;            // full array
    size_t   l, r;         // slice range
    int32_t* g;            // shared global max
    pthread_mutex_t* mx;   // shared mutex
    int      nosync;       // if 1, skip lock (to test race condition)
} TArg;

// thread worker: finds local max, updates shared global max
static void* th_max(void* arg) {
    TArg* x = (TArg*)arg;
    int32_t loc = INT_MIN;

    for (size_t i = x->l; i < x->r; i++) {
        if (x->a[i] > loc) loc = x->a[i];
    }

    if (x->nosync) {
        if (loc > *x->g) *x->g = loc; // unsafe update, for testing nosync
    }
    else {
        pthread_mutex_lock(x->mx);
        if (loc > *x->g) *x->g = loc; // safe update
        pthread_mutex_unlock(x->mx);
    }

    return NULL;
}

// runs the MapReduce process using threads
static void run_threads(int32_t* a, size_t N, int W, int nosync,
    int32_t* out, double* tmap, double* tred) {
    double t0 = now_s();

    pthread_t th[8];
    TArg      ar[8];
    pthread_mutex_t mx;
    pthread_mutex_init(&mx, NULL);

    int32_t g = INT_MIN;
    size_t step = (N + W - 1) / W;

    // create threads
    for (int w = 0; w < W; w++) {
        size_t l = (size_t)w * step;
        size_t r = (l + step > N) ? N : (l + step);
        ar[w].a = a;
        ar[w].l = l;
        ar[w].r = r;
        ar[w].g = &g;
        ar[w].mx = &mx;
        ar[w].nosync = nosync;
        pthread_create(&th[w], NULL, th_max, &ar[w]);
    }

    // wait for all threads
    for (int w = 0; w < W; w++) {
        pthread_join(th[w], NULL);
    }

    double t1 = now_s();
    *out = g;
    double t2 = now_s();

    *tmap = t1 - t0;
    *tred = t2 - t1;

    pthread_mutex_destroy(&mx);
}

/* ---------------- PROCESSES VERSION ---------------- */

// process version: uses shared memory and semaphores
static void run_procs(int32_t* a, size_t N, int W, int nosync,
    int32_t* out, double* tmap, double* tred) {
    double t0 = now_s();

    // create shared memory region for one integer
    int shm = shm_open("/mr_one_int", O_CREAT | O_RDWR, 0600);
    if (shm == -1) { perror("shm_open"); exit(1); }
    if (ftruncate(shm, (off_t)sizeof(int32_t)) == -1) {
        perror("ftruncate");
        exit(1);
    }

    int32_t* g = (int32_t*)mmap(NULL, sizeof(int32_t),
        PROT_READ | PROT_WRITE, MAP_SHARED, shm, 0);
    if (g == MAP_FAILED) { perror("mmap"); exit(1); }
    *g = INT_MIN;

    // semaphore for synchronization
    sem_t* sem = sem_open("/mr_one_sem", O_CREAT | O_EXCL, 0600, 1);
    if (sem == SEM_FAILED) {
        sem = sem_open("/mr_one_sem", 0);
        if (sem == SEM_FAILED) { perror("sem_open"); exit(1); }
    }

    pid_t pid[8];
    size_t step = (N + W - 1) / W;

    // fork processes
    for (int w = 0; w < W; w++) {
        size_t l = (size_t)w * step;
        size_t r = (l + step > N) ? N : (l + step);

        pid[w] = fork();
        if (pid[w] < 0) { perror("fork"); exit(1); }

        if (pid[w] == 0) {
            int32_t loc = INT_MIN;
            for (size_t i = l; i < r; i++) {
                if (a[i] > loc) loc = a[i];
            }

            if (nosync) {
                if (loc > *g) *g = loc;
            }
            else {
                sem_wait(sem);
                if (loc > *g) *g = loc;
                sem_post(sem);
            }
            _exit(0);
        }
    }

    // wait for all processes
    for (int w = 0; w < W; w++) {
        int st = 0;
        waitpid(pid[w], &st, 0);
    }

    double t1 = now_s();
    *out = *g;
    double t2 = now_s();

    *tmap = t1 - t0;
    *tred = t2 - t1;

    // cleanup
    munmap(g, sizeof(int32_t));
    close(shm);
    shm_unlink("/mr_one_int");
    sem_close(sem);
    sem_unlink("/mr_one_sem");
}

/* ---------------- MAIN ---------------- */

int main(int argc, char** argv) {
    if (argc < 4) {
        fprintf(stderr, "Usage: %s <threads|procs> <workers> <N> [nosync]\n", argv[0]);
        return 1;
    }

    bool threads = (strcmp(argv[1], "threads") == 0);
    int  W = atoi(argv[2]);
    size_t N = (size_t)atol(argv[3]);
    int  nosync = (argc >= 5 && strcmp(argv[4], "nosync") == 0);

    if (!(W == 1 || W == 2 || W == 4 || W == 8)) {
        fprintf(stderr, "workers must be 1, 2, 4, or 8\n");
        return 1;
    }

    // allocate array and fill it with random data
    int32_t* a = (int32_t*)malloc(N * sizeof(int32_t));
    if (!a) { perror("malloc"); return 1; }
    fill_rand(a, N, 42u);

    double tmap = 0.0, tred = 0.0;
    double t0 = now_s();

    int32_t g = INT_MIN;

    // choose mode
    if (threads)
        run_threads(a, N, W, nosync, &g, &tmap, &tred);
    else
        run_procs(a, N, W, nosync, &g, &tmap, &tred);

    double tall = now_s() - t0;

    // print results
    printf("Global Max = %d\n", g);
    if (N <= 131072)
        printf("Check (serial) = %d\n", serial_max(a, N));
    printf("Map    : %.6f s\n", tmap);
    printf("Reduce : %.6f s\n", tred);
    printf("Total  : %.6f s\n", tall);

    free(a);
    return 0;
}
