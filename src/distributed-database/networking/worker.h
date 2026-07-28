#ifndef WORKER_H
#define WORKER_H

#include "networking/msg.h"
#include "networking/rpc.h"

/**
 * Queue a message to be sent to this node from its own worker thread
 * @param node the node to send the message to
 * @param msg the message to send
 */
extern void queueSend(NetworkNode node, Msg msg);

/**
 * Queue a message, received from this node, to be executed on its own
 * worker thread
 * @param node the node that sent the message
 * @param msg the message to execute
 */
extern void queueExecute(NetworkNode node, Msg msg);

/**
 * pthread entry point for a peer's send worker thread: drains that node's
 * sendQueue, sending jobs as they arrive. Blocks the thread it is run in.
 * @param arg the NetworkNode whose sendQueue this thread drains
 */
extern void *runSendWorker(void *arg);

/**
 * pthread entry point for a peer's execute worker thread: drains that
 * node's executeQueue, executing jobs as they arrive. Blocks the thread it
 * is run in.
 * @param arg the NetworkNode whose executeQueue this thread drains
 */
extern void *runExecuteWorker(void *arg);

#endif  // WORKER_H
