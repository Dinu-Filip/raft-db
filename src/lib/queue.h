#ifndef QUEUE_H
#define QUEUE_H

#include <stdbool.h>

typedef struct QueueNode *QueueNode;
struct QueueNode {
    void *value;
    QueueNode next;
};

typedef struct Queue *Queue;
struct Queue {
    QueueNode head;
    QueueNode tail;
};

/**
 * @brief Creates the Queue on the heap
 * @return The created Queue
 */
extern Queue allocQueue();

/**
 * @brief Deallocates memory used by the queue. Note: Queue must be empty before
 * freeing
 * @param queue Queue to deallocate memory for.
 */
extern void freeQueue(Queue queue);

/**
 * @brief Enqueues value to queue
 * @param queue Queue to add enqueue element to
 * @param value Value to enqueue to queue
 * @return 0 if operation successful and -1 if unsuccessful
 */
extern int enqueue(Queue queue, void *value);

/**
 * @brief Queue to dequeue element from
 * @param queue Queue to dequeue element to
 * @return Value dequeued from queue
 */
extern void *dequeue(Queue queue);

/**
 * @brief Check if queue is empty
 * @param queue Queue to check if is empty
 * @return Bool which is true if and only if queue is empty
 */
extern bool isQueueEmpty(Queue queue);

#endif  // QUEUE_H
