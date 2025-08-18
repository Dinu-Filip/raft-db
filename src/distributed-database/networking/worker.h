#ifndef WORKER_H
#define WORKER_H

#include "networking/msg.h"
#include "networking/rpc.h"

/**
 * Initialise the worker to send and execute messages
 */
extern void initialiseWorker();

/**
 * Queue a message to be sent from the main thread
 * @param node the node to send the message to
 * @param msg the message to send
 */
extern void queueSend(NetworkNode node, Msg msg);

/**
 * Queue a message to be sent to all nodes from the main thread
 * @param msg the message to send
 */
extern void queueSendAll(Msg msg);

/**
 * Queue a message to be executed on the main thread
 * @param msg the message to execute
 * @param senderId the id of the node that send the message
 */
extern void queueExecute(Msg msg, int senderId);

/**
 * Run the worker to send and execute messages, blocks the thread that it is run
 * in
 */
extern void runWorker();

#endif  // WORKER_H
