
This project implements two MapReduce-style programs using multithreading and multiprocessing to simulate distributed computation on a single machine.  
Both programs demonstrate parallelism,Inter Process Communication(IPC), and synchronization using C.

To build these codes:
gcc -O2 -pthread ParallelSortingMapReduce.c -o ParallelSortingMapReduce
gcc -O2 -pthread MaxValueMapReduce.c -o MaxValueMapReduce

And running:
./ParallelSortingMapReduce threads 1 32
./ParallelSortingMapReduce threads 8 131072
./ParallelSortingMapReduce procs   8 131072

./MaxValueMapReduce threads 1 32
./MaxValueMapReduce threads 4 131072
./MaxValueMapReduce threads 8 131072
./MaxValueMapReduce procs   8 131072
.
