#include "start.h"

#include <pthread.h>
#include <signal.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>

#include "client-handling/server.h"
#include "log.h"
#include "networking/rpc.h"
#include "networking/worker.h"
#include "raft/raft-node.h"
#include "raft/raft.h"

#define DIR_MODE 0755

int start(int argc, char **argv) {
    if (argc < 3) {
        fprintf(stderr,
                "Format: databasenode <CLIENT_HANDLING_PORT> <NODE_COUNT> "
                "<NODE_0> ... <NODE_N> <PORT>\n");
        return EXIT_FAILURE;
    }

    int clientHandlingPort;
    if (sscanf(argv[1], "%d", &clientHandlingPort) != 1) {
        fprintf(stderr, "Failed to read client handling port integer from %s\n",
                *argv);
        return EXIT_FAILURE;
    }

    int nodeCount;
    if (sscanf(argv[2], "%d", &nodeCount) != 1) {
        fprintf(stderr, "Failed to read node count integer from %s\n", *argv);
        return EXIT_FAILURE;
    }

    // Check if the final argument is an address
    int nAddrs = argc == 3 ? 0 : argc - (strchr(argv[argc - 1], ':') ? 3 : 4);
    int nodeId = nAddrs;
    bool runServer = nodeId != nodeCount - 1;

    if (nodeId >= nodeCount) {
        fprintf(stderr,
                "Too many nodes have been provided for the given node count\n");
        return EXIT_FAILURE;
    } else if (runServer && argc != nAddrs + 4) {
        fprintf(stderr,
                "Current node is not final node so must have a port to run "
                "server on\n");
        return EXIT_FAILURE;
    }

    int port;
    if (runServer && sscanf(argv[argc - 1], "%d", &port) != 1) {
        fprintf(stderr, "Failed to read port integer from %s\n", *argv);
        return EXIT_FAILURE;
    }

    mkdir("raft-db", DIR_MODE);
    char dir[32];
    sprintf(dir, "raft-db/%d", nodeId);
    mkdir(dir, DIR_MODE);
    sprintf(dir, "raft-db/%d/data", nodeId);
    mkdir(dir, DIR_MODE);

    LOG("Starting RPC server as node %d (out of %d nodes)", nodeId, nodeCount);

    signal(SIGINT, cleanUpServer);

    initRaftNode(nodeId, nodeCount);
    initialiseWorker();
    initialiseRpc(nodeId, nodeCount);
    startClients(nAddrs, argv + 3);
    initDatabasePath(nodeId);
    if (runServer) startServer(port);

    pthread_t clientHandlingServerThread;
    pthread_create(&clientHandlingServerThread, NULL, runClientHandlingServer,
                   &clientHandlingPort);

    pthread_t nodePingThread;
    pthread_create(&nodePingThread, NULL, startPingThread, NULL);

    pthread_t raftThread;
    pthread_create(&raftThread, NULL, runRaftMain, NULL);

    runWorker();

    return EXIT_SUCCESS;
}
