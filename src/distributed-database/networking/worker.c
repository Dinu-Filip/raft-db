#include "worker.h"

#include <assert.h>
#include <stdio.h>
#include <stdlib.h>

#include "concurrent/queue.h"
#include "networking/execute.h"
#include "networking/msg.h"
#include "networking/rpc.h"

static ConcurrentQueue queue;

typedef enum {
    SEND,
    SEND_ALL,
    EXECUTE,
} JobType;

typedef struct Job *Job;
struct Job {
    JobType type;
    union {
        struct {
            NetworkNode node;
            Msg msg;
        } send;
        struct {
            Msg msg;
        } sendAll;
        struct {
            Msg msg;
            int senderId;
        } execute;
    } data;
};

void initialiseWorker() { queue = createConcurrentQueue(); }

void queueSend(NetworkNode node, Msg msg) {
    Job job = malloc(sizeof(struct Job));
    assert(job != NULL);

    job->type = SEND;
    job->data.send.node = node;
    job->data.send.msg = msg;

    concurrentEnqueue(queue, job);
}

void queueSendAll(Msg msg) {
    Job job = malloc(sizeof(struct Job));
    assert(job != NULL);

    job->type = SEND_ALL;
    job->data.sendAll.msg = msg;

    concurrentEnqueue(queue, job);
}

void queueExecute(Msg msg, int senderId) {
    Job job = malloc(sizeof(struct Job));
    assert(job != NULL);

    job->type = EXECUTE;
    job->data.execute.msg = msg;
    job->data.execute.senderId = senderId;

    concurrentEnqueue(queue, job);
}

void runWorker() {
    for (;;) {
        Job job = concurrentDequeueWait(queue);

        switch (job->type) {
            case SEND:
                sendMsg(job->data.send.node, job->data.send.msg);
                freeMsgShallow(job->data.send.msg);
                break;
            case SEND_ALL:
                sendMsgAll(job->data.sendAll.msg);
                freeMsgShallow(job->data.sendAll.msg);
                break;
            case EXECUTE:
                execute(job->data.execute.msg, job->data.execute.senderId);
                freeMsgShallow(job->data.execute.msg);
                break;
        }

        free(job);
    }
}
