#include "worker.h"

#include <assert.h>
#include <stdio.h>
#include <stdlib.h>

#include "concurrent/queue.h"
#include "networking/execute.h"
#include "networking/msg.h"
#include "networking/rpc.h"
#include "utils.h"

typedef struct Job *Job;
struct Job {
    NetworkNode node;
    Msg msg;
};

static Job createJob(NetworkNode node, Msg msg) {
    Job job = malloc(sizeof(struct Job));
    assert(job != NULL);

    job->node = node;
    job->msg = msg;

    return job;
}

void queueSend(NetworkNode node, Msg msg) {
    concurrentEnqueue(node->sendQueue, createJob(node, msg));
}

void queueExecute(NetworkNode node, Msg msg) {
    concurrentEnqueue(node->executeQueue, createJob(node, msg));
}

void *runSendWorker(void *arg) {
    NetworkNode node = (NetworkNode)arg;

    for (;;) {
        Job job = concurrentDequeueWait(node->sendQueue);

        sendMsg(job->node, job->msg);

        freeMsgShallow(job->msg);
        free(job);
    }

    return NULL;
}

void *runExecuteWorker(void *arg) {
    NetworkNode node = (NetworkNode)arg;

    for (;;) {
        Job job = concurrentDequeueWait(node->executeQueue);
        uint64_t dequeuedAtNs = monotonicNs();

        execute(job->msg, job->node->id, dequeuedAtNs);

        freeMsgShallow(job->msg);
        free(job);
    }

    return NULL;
}
