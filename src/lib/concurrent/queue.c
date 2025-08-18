#include "concurrent/queue.h"

#include <assert.h>
#include <pthread.h>
#include <stdbool.h>
#include <stdlib.h>

typedef struct QueueNode *QueueNode;
struct QueueNode {
    QueueNode next;
    VALUE value;
};

typedef struct ConcurrentQueue *ConcurrentQueue;
struct ConcurrentQueue {
    pthread_mutex_t mutex;
    pthread_cond_t cond;
    QueueNode front;
    QueueNode end;
};

static pthread_mutex_t *getMutex(ConcurrentQueue q) { return &q->mutex; }
static pthread_cond_t *getCond(ConcurrentQueue q) { return &q->cond; }

ConcurrentQueue createConcurrentQueue() {
    ConcurrentQueue q = malloc(sizeof(struct ConcurrentQueue));
    assert(q != NULL);

    pthread_mutex_init(getMutex(q), NULL);
    pthread_cond_init(getCond(q), NULL);
    q->front = NULL;
    q->end = NULL;

    return q;
}

// Mutex must be unlocked before freeing queue
void freeConcurrentQueue(ConcurrentQueue q) {
    pthread_mutex_destroy(getMutex(q));
    pthread_cond_destroy(getCond(q));

    QueueNode node = q->front;
    while (node != NULL) {
        QueueNode next = node->next;
        free(node);
        node = next;
    }

    free(q);
}

static QueueNode createQueueNode(VALUE value) {
    QueueNode node = malloc(sizeof(struct QueueNode));
    assert(node != NULL);

    node->value = value;
    node->next = NULL;

    return node;
}

void concurrentEnqueue(ConcurrentQueue q, VALUE value) {
    QueueNode node = createQueueNode(value);

    pthread_mutex_lock(getMutex(q));

    if (q->end == NULL) {
        // front should be NULL if end is NULL
        assert(q->front == NULL);

        q->front = node;
        q->end = node;
    } else {
        q->end->next = node;
        q->end = node;
    }

    pthread_cond_signal(getCond(q));
    pthread_mutex_unlock(getMutex(q));
}

static bool isEmpty(ConcurrentQueue q) { return q->front == NULL; }

static VALUE dequeue(ConcurrentQueue q) {
    if (isEmpty(q)) {
        return NULL;
    }

    QueueNode front = q->front;

    q->front = q->front->next;

    if (q->front == NULL) {
        q->end = NULL;
    }

    VALUE value = front->value;

    free(front);

    return value;
}

VALUE concurrentDequeue(ConcurrentQueue q) {
    pthread_mutex_lock(getMutex(q));
    VALUE value = dequeue(q);
    pthread_mutex_unlock(getMutex(q));

    return value;
}

VALUE concurrentDequeueWait(ConcurrentQueue q) {
    pthread_mutex_lock(getMutex(q));

    while (isEmpty(q)) {
        pthread_cond_wait(getCond(q), getMutex(q));
    }
    VALUE value = dequeue(q);

    pthread_mutex_unlock(getMutex(q));

    return value;
}

VALUE concurrentPeek(ConcurrentQueue q) {
    pthread_mutex_lock(getMutex(q));

    if (isEmpty(q)) {
        return NULL;
    }

    VALUE value = q->front->value;

    pthread_mutex_unlock(getMutex(q));

    return value;
}

bool concurrentIsEmpty(ConcurrentQueue q) {
    pthread_mutex_lock(getMutex(q));

    bool result = isEmpty(q);

    pthread_mutex_unlock(getMutex(q));

    return result;
}
