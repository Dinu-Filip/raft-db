#ifndef CONCURRENT_QUEUE_H
#define CONCURRENT_QUEUE_H

#include <stdbool.h>

#define VALUE void *
#define CONCURRENT_QUEUE_MEMORY_FAILURE 1

typedef struct ConcurrentQueue *ConcurrentQueue;

extern ConcurrentQueue createConcurrentQueue(void);
extern void freeConcurrentQueue(ConcurrentQueue queue);

// Returns 0 on success
extern void concurrentEnqueue(ConcurrentQueue queue, VALUE value);
extern VALUE concurrentDequeue(ConcurrentQueue queue);
extern VALUE concurrentDequeueWait(ConcurrentQueue queue);
extern VALUE concurrentPeek(ConcurrentQueue queue);
extern bool concurrentIsEmpty(ConcurrentQueue queue);

#endif  // CONCURRENT_QUEUE_H
