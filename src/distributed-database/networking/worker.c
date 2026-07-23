#include "worker.h"

#include <assert.h>
#include <stdio.h>
#include <stdlib.h>

#include "concurrent/queue.h"
#include "networking/execute.h"
#include "networking/msg.h"
#include "networking/rpc.h"

typedef enum {
    SEND,
    EXECUTE,
} JobType;

typedef struct Job *Job;
struct Job {
    JobType type;
    NetworkNode node;
    Msg msg;
};

void queueSend(NetworkNode node, Msg msg) {
    Job job = malloc(sizeof(struct Job));
    assert(job != NULL);

    job->type = SEND;
    job->node = node;
    job->msg = msg;

    concurrentEnqueue(node->queue, job);
}

void queueExecute(NetworkNode node, Msg msg) {
    Job job = malloc(sizeof(struct Job));
    assert(job != NULL);

    job->type = EXECUTE;
    job->node = node;
    job->msg = msg;

    concurrentEnqueue(node->queue, job);
}

void *runNodeWorker(void *arg) {
    NetworkNode node = (NetworkNode)arg;

    for (;;) {
        Job job = concurrentDequeueWait(node->queue);

        switch (job->type) {
            case SEND:
                sendMsg(job->node, job->msg);
                break;
            case EXECUTE:
                execute(job->msg, job->node->id);
                break;
        }

        freeMsgShallow(job->msg);
        free(job);
    }

    return NULL;
}
