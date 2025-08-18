#include "queue.h"

#include <assert.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>

Queue allocQueue() {
    const Queue queue = malloc(sizeof(struct Queue));

    if (queue == NULL) {
        return NULL;
    }

    queue->head = NULL;
    queue->tail = NULL;

    return queue;
}

void freeQueue(Queue queue) {
    assert(queue != NULL);
    assert(isQueueEmpty(queue));

    free(queue);
}

int enqueue(Queue queue, void *value) {
    assert(queue != NULL);

    QueueNode newNode = malloc(sizeof(struct QueueNode));

    if (newNode == NULL) {
        return -1;
    }

    newNode->value = value;
    newNode->next = NULL;

    if (queue->tail != NULL) {
        queue->tail->next = newNode;
    }
    queue->tail = newNode;

    if (queue->head == NULL) {
        queue->head = newNode;
    }

    return 0;
}

void *dequeue(Queue queue) {
    assert(queue != NULL);

    if (queue->head == NULL) {
        return NULL;
    }

    void *output = queue->head->value;

    QueueNode temp = queue->head;

    queue->head = queue->head->next;

    free(temp);

    if (queue->head == NULL) {
        queue->tail = NULL;
    }

    return output;
}

bool isQueueEmpty(Queue queue) { return queue->head == NULL; }
