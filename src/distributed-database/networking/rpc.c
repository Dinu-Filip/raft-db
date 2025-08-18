#include "rpc.h"

#include <arpa/inet.h>
#include <assert.h>
#include <errno.h>
#include <netinet/in.h>
#include <pthread.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>

#include "log.h"
#include "networking/worker.h"

#define RECONNECT_DELAY_US 100000
#define MAX_LOG_RECONNECT 10
#define PING_DELAY 1
#define PING_DISCONNECT 5

#define NULL_FD -1
#define WRITE_ERROR -1
#define READ_EOF 0
#define READ_ERROR -1
#define AUTO_SOCKET_PROTOCOL 0
#define SOCKET_ERROR -1
#define CONNECT_ERROR -1
#define ACCEPT_ERROR -1
#define BIND_ERROR -1
#define LISTEN_ERROR -1

static struct NetworkNode *nodes;
static int selfId;
static int nodeCount;
static pthread_t serverAcceptThread;
static int serverSockFd = NULL_FD;

static int getNodeIndex(int nodeId) {
    assert(nodeId != selfId);
    return nodeId < selfId ? nodeId : nodeId - 1;
}
static NetworkNode getNode(int nodeId) { return &nodes[getNodeIndex(nodeId)]; }

void initialiseRpc(int id, int count) {
    selfId = id;
    nodeCount = count;
    nodes = malloc((nodeCount - 1) * sizeof(struct NetworkNode));
    assert(nodes != NULL);
    for (int i = 0; i < nodeCount; i++) {
        if (i == selfId) continue;
        NetworkNode node = getNode(i);
        node->id = i;
        node->state = DISCONNECTED;
        node->connectionAttempts = 0;
        pthread_mutex_init(&node->mutex, NULL);
        time(&node->lastPing);
    }
}

void nodeSend(int nodeId, Msg msg) {
    NetworkNode node = getNode(nodeId);
    queueSend(node, msg);
}

void nodeSendAll(Msg msg) { queueSendAll(msg); }

static ssize_t writeBytes(NetworkNode node, uint8_t *buff, size_t bytesCount) {
    ssize_t totalBytesWritten = 0;
    while (totalBytesWritten < bytesCount) {
        ssize_t bytesWritten =
            write(node->fd, buff, bytesCount - totalBytesWritten);

        if (bytesWritten == WRITE_ERROR) {
            if (errno == EAGAIN || errno == EWOULDBLOCK || errno == ETIMEDOUT) {
                if (node->lastPing < time(NULL) - PING_DISCONNECT) {
                    return WRITE_ERROR;
                }
            } else {
                perror("Failed to write");
                return WRITE_ERROR;
            }
        }

        totalBytesWritten += bytesWritten;
        buff += bytesWritten;
    }
    return totalBytesWritten;
}

static void disconnectNode(NetworkNode node) {
    node->state = DISCONNECTED;
    close(node->fd);
    node->fd = NULL_FD;
}

static void disconnectNodeSafe(NetworkNode node) {
    pthread_mutex_lock(&node->mutex);
    disconnectNode(node);
    pthread_mutex_unlock(&node->mutex);
}

static void sendMsgEncoded(NetworkNode node, EncodeRes res) {
    pthread_mutex_lock(&node->mutex);
    if (node->state == DISCONNECTED) {
        pthread_mutex_unlock(&node->mutex);
        return;
    }

    writeBytes(node, res->buff, res->size);

    pthread_mutex_unlock(&node->mutex);
}

void sendMsg(NetworkNode node, Msg msg) {
    EncodeRes res = encode(msg);

    sendMsgEncoded(node, res);

    free(res->buff);
    free(res);
}

void sendMsgAll(Msg msg) {
    EncodeRes res = encode(msg);

    for (int i = 0; i < nodeCount; i++) {
        if (i == selfId) continue;
        NetworkNode node = getNode(i);
        sendMsgEncoded(node, res);
    }

    free(res->buff);
    free(res);
}

void processPing(int nodeId) {
    NetworkNode node = getNode(nodeId);
    time(&node->lastPing);
}

int checkReadBuff(ReadBuff readBuff, int nBytes) {
    for (;;) {
        long currPos = readBuff->buff - readBuff->base;
        long availableBytes = readBuff->size - currPos;
        if (availableBytes >= nBytes) break;

        long freeBytes = readBuff->capacity - readBuff->size;

        if (freeBytes + availableBytes < nBytes) {
            if (readBuff->capacity < nBytes) {
                readBuff->capacity = nBytes;
                readBuff->base = malloc(readBuff->capacity);
                assert(readBuff->base != NULL);
            }

            memmove(readBuff->base, readBuff->buff, availableBytes);
            readBuff->buff = readBuff->base;
            readBuff->size = availableBytes;

            freeBytes = readBuff->capacity - readBuff->size;
        }

        ssize_t bytesRead =
            read(readBuff->fd, readBuff->base + readBuff->size, freeBytes);
        if (bytesRead == READ_EOF || bytesRead == READ_ERROR) {
            return CHECK_READ_BUFF_FAIL;
        }

        readBuff->size += bytesRead;
    }

    return CHECK_READ_BUFF_SUCCESS;
}

static void nodeListen(NetworkNode node) {
    ReadBuff readBuff = createReadBuff(node->fd);

    for (;;) {
        Msg msg = parse(readBuff);

        if (msg == NULL) {
            // This also triggers when an invalid message is sent
            LOG("Lost connection to node %d", node->id);
            disconnectNodeSafe(node);
            // For client connections, this is called from inside reconnect, so
            // will attempt to reconnect after exiting, for server connections,
            // the thread will exit, allowing a new thread to be made when the
            // node reconnects
            break;
        }

        // Identify is a special case that is handled internally
        if (msg->type == SERVER_IDENTIFY) {
            LOG("Node %d has identified as node %d", node->id,
                msg->data.identify.id);
            if (node->id != msg->data.identify.id) {
                // Kill the whole program if any nodes have a different id to
                // what we expect
                LOG_ERROR("Node gave incorrect identification");
            }
            freeMsgShallow(msg);
        } else {
            queueExecute(msg, node->id);
        }
    }

    freeReadBuff(readBuff);
}

static void setWriteTimeout(int fd) {
    struct timeval time = {
        .tv_sec = 1,
        .tv_usec = 0,
    };
    setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &time, sizeof(time));
}

static void clientConnect(NetworkNode node) {
    node->connectionAttempts++;
    bool enableLog = node->connectionAttempts < MAX_LOG_RECONNECT;

    if (enableLog) {
        LOG("Creating client socket for node %d", node->id);
    }
    int fd = socket(AF_INET, SOCK_STREAM, AUTO_SOCKET_PROTOCOL);
    if (fd == SOCKET_ERROR) {
        LOG_PERROR("Failed to create socket");
    }

    setWriteTimeout(fd);

    if (enableLog) {
        LOG("Connecting client to node %d (%s:%hd) (attempt %d)", node->id,
            node->ip, node->port, node->connectionAttempts);
    }

    if (connect(fd, (struct sockaddr *)&node->addr, sizeof(node->addr)) ==
        CONNECT_ERROR) {
        if (enableLog) perror("Failed to connect client to node");
        close(fd);
        return;
    }

    if (enableLog) {
        LOG("Successfully connected client to node");
    }

    node->connectionAttempts = 0;

    pthread_mutex_lock(&node->mutex);
    node->fd = fd;
    node->state = CONNECTED;
    time(&node->lastPing);
    pthread_mutex_unlock(&node->mutex);

    Msg identifyMsg = malloc(sizeof(struct Msg));
    identifyMsg->type = CLIENT_IDENTIFY;
    identifyMsg->data.identify.id = selfId;

    nodeSend(node->id, identifyMsg);
}

static void clientReconnect(NetworkNode node) {
    while (node->state == DISCONNECTED) {
        clientConnect(node);
        if (node->state == CONNECTED) {
            nodeListen(node);
        } else {
            usleep(RECONNECT_DELAY_US);
        }
    }
}

static void *clientRunThread(void *arg) {
    NetworkNode node = (NetworkNode)arg;
    clientReconnect(node);
    return NULL;
}

void startClients(int addrCount, char **addrs) {
    for (int i = 0; i < addrCount; i++, addrs++) {
        // An ip can be at most 15 characters long
        char ip[16];
        uint16_t port;
        if (sscanf(*addrs, "%15[^:]:%hu", ip, &port) != 2) {
            LOG_ERROR("Failed to read ip and port from %s", *addrs);
        }

        NetworkNode node = getNode(i);
        node->port = port;
        strcpy(node->ip, ip);
        node->addr.sin_family = AF_INET;
        node->addr.sin_addr.s_addr = inet_addr(ip);
        node->addr.sin_port = htons(port);

        pthread_create(&node->thread, NULL, clientRunThread, node);
    }
}

void startPings() {
    for (;;) {
        Msg msg = malloc(sizeof(struct Msg));
        msg->type = PING;
        nodeSendAll(msg);

        time_t lastPingTime = time(NULL) - PING_DISCONNECT;

        for (int i = 0; i < nodeCount; i++) {
            if (i == selfId) continue;
            NetworkNode node = getNode(i);

            pthread_mutex_lock(&node->mutex);

            if (node->state == CONNECTED && node->lastPing < lastPingTime) {
                LOG("Node %d hasn't responded to pings, killing connection",
                    node->id);
                pthread_cancel(node->thread);
                disconnectNode(node);
                if (node->id < selfId) {
                    pthread_create(&node->thread, NULL, clientRunThread, node);
                }
            }

            pthread_mutex_unlock(&node->mutex);
        }

        sleep(PING_DELAY);
    }
}

void *startPingThread(void *arg) {
    startPings();
    return NULL;
}

typedef struct UnknownClientState *UnknownClientState;
struct UnknownClientState {
    int connfd;
    pthread_t thread;
};

static void serverWaitForIdentify(UnknownClientState clientState) {
    int connfd = clientState->connfd;
    pthread_t thread = clientState->thread;
    free(clientState);

    ReadBuff readBuff = createReadBuff(connfd);
    Msg msg = parse(readBuff);
    freeReadBuff(readBuff);

    if (msg == NULL || msg->type != CLIENT_IDENTIFY) {
        LOG("Unknown node didn't identify");
        close(connfd);
        freeMsgShallow(msg);
        return;
    }

    LOG("Unknown node has identified as node %d", msg->data.identify.id);

    NetworkNode node = getNode(msg->data.identify.id);
    freeMsgShallow(msg);

    if (node->state == CONNECTED) {
        LOG("Connected node has attempted to reconnect before closing old "
            "connection");
        close(connfd);
        return;
    }

    pthread_mutex_lock(&node->mutex);
    node->fd = connfd;
    node->thread = thread;
    node->state = CONNECTED;
    time(&node->lastPing);
    pthread_mutex_unlock(&node->mutex);

    Msg identifyMsg = malloc(sizeof(struct Msg));
    identifyMsg->type = SERVER_IDENTIFY;
    identifyMsg->data.identify.id = selfId;

    nodeSend(node->id, identifyMsg);

    nodeListen(node);
}

static void *serverRunNodeThread(void *arg) {
    UnknownClientState clientState = (UnknownClientState)arg;
    serverWaitForIdentify(clientState);
    return NULL;
}

static void serverAcceptConnections() {
    for (;;) {
        int connfd = accept(serverSockFd, NULL, NULL);
        if (connfd == ACCEPT_ERROR) {
            LOG_PERROR("Failed to accept server connection");
        }

        setWriteTimeout(connfd);

        LOG("Accepted new server connection, waiting for identify");

        UnknownClientState clientState =
            malloc(sizeof(struct UnknownClientState));
        clientState->connfd = connfd;
        pthread_create(&clientState->thread, NULL, serverRunNodeThread,
                       clientState);
    }
}

static void *serverRunAcceptThread(void *arg) {
    serverAcceptConnections();
    return NULL;
}

void cleanUpServer() {
    for (int i = 0; i < nodeCount; i++) {
        if (i == selfId) continue;
        NetworkNode node = getNode(i);
        pthread_mutex_lock(&node->mutex);
        if (node->state == DISCONNECTED) continue;
        LOG("Closing connection to node %d", node->id);
        close(node->fd);
        pthread_mutex_unlock(&node->mutex);
    }
    if (serverSockFd != NULL_FD) {
        LOG("Closing RPC server");
        close(serverSockFd);
    }
    exit(0);
}

void startServer(uint16_t port) {
    LOG("Creating socket for server");
    serverSockFd = socket(AF_INET, SOCK_STREAM, 0);
    if (serverSockFd == SOCKET_ERROR) {
        LOG_PERROR("Failed to create socket");
    }

    int opt = 1;
    setsockopt(serverSockFd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    struct sockaddr_in addr = {
        .sin_family = AF_INET,
        .sin_addr.s_addr = htonl(INADDR_ANY),
        .sin_port = htons(port),
    };

    LOG("Starting server on port %d", port);
    if (bind(serverSockFd, (struct sockaddr *)&addr, sizeof(addr)) ==
        BIND_ERROR) {
        LOG_PERROR("Failed to start server");
    }

    if (listen(serverSockFd, 5) == LISTEN_ERROR) {
        LOG_PERROR("Failed to listen on socket");
    }

    pthread_create(&serverAcceptThread, NULL, serverRunAcceptThread, NULL);
}
