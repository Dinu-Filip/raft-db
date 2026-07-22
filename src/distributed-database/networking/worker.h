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
 * pthread entry point for a peer's worker thread: drains that node's queue,
 * sending/executing jobs as they arrive. Blocks the thread it is run in.
 * @param arg the NetworkNode whose queue this thread drains
 */
extern void *runNodeWorker(void *arg);

#endif  // WORKER_H
