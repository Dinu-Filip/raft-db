#ifndef RPC_H
#define RPC_H

#include <netinet/in.h>
#include <pthread.h>  // IWYU pragma: keep
#include <stddef.h>
#include <stdint.h>
#include <sys/types.h>

#include "networking/msg.h"

typedef enum {
    CONNECTED,
    DISCONNECTED,
} ConnectionState;

typedef struct NetworkNode *NetworkNode;
struct NetworkNode {
    int id;
    char ip[16];
    uint16_t port;
    ConnectionState state;
    struct sockaddr_in addr;
    int fd;
    pthread_t thread;
    pthread_mutex_t mutex;
    int connectionAttempts;
    time_t lastPing;
};

/**
 * Initialise the RPC to send and recieve messages
 * @param nodeId the id that this process is running
 * @param nodeCount the number of nodes in total
 */
extern void initialiseRpc(int nodeId, int nodeCount);

/**
 * Queues a message to be sent to the specified node if it is connected
 * @param nodeId the id of the node to send the message to
 * @param msg the message to send
 */
extern void nodeSend(int id, Msg msg);

/**
 * Queues a message to be sent to every node
 * @param msg the message to send
 */
extern void nodeSendAll(Msg msg);

/**
 * Send a message to a node
 * @param node the node to send the message to
 * @param msg the message to send
 */
extern void sendMsg(NetworkNode node, Msg msg);

/**
 * Send a message to all nodes
 * @param msg the message to send
 */
extern void sendMsgAll(Msg msg);

/**
 * Process a ping recieved from a node
 * @param nodeId the node that sent the ping
 */
extern void processPing(int nodeId);

#define CHECK_READ_BUFF_SUCCESS 0
#define CHECK_READ_BUFF_FAIL -1

/**
 * Check if the read buffer contains the required number of bytes, if not, read
 * from the node until the given number of bytes are available, returns 0 for
 * success and -1 for fail
 * @param readBuff the read buffer
 * @param nBytes the number of bytes needed
 */
extern int checkReadBuff(ReadBuff readBuff, int nBytes);

/**
 * Connects and listens for messages from each node's RPC server in the addrs
 * list, connects to each one in a seperate thread and always exits
 * @param addrCount the number of nodes to read from addrs
 * @param addrs the ips and ports to connect to in the format <ip>:<port>
 */
extern void startClients(int addrCount, char **addrs);

/**
 * Start the thread sending pings to all nodes
 * @param arg the args passed to the run function, should be NULL
 */
extern void *startPingThread(void *arg);

/**
 * Closes the server and all connections to nodes
 */
extern void cleanUpServer();

/**
 * Starts the RPC server on the given port in a new thread
 * @param port the port to run the RPC server on
 */
extern void startServer(uint16_t port);

#endif  // RPC_H
