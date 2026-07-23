#include "server.h"

#include <assert.h>
#include <math.h>
#include <stdatomic.h>
#include <stdbool.h>
#include <stdlib.h>
#include <sys/time.h>
#include <third-party/mongoose.h>

#include "client-handling/input.h"
#include "log.h"
#include "networking/msg.h"
#include "raft/callbacks.h"
#include "raft/raft-node.h"

#define OK_RESPONSE_CODE 200
#define BAD_REQUEST_RESPONSE_CODE 400
#define NOT_FOUND_RESPONSE_CODE 404
#define METHOD_NOT_ALLOWED_RESPONSE_CODE 405

// Backstop for mg_mgr_poll's timeout; normally woken sooner by socket
// activity or notifyWriteProgress()'s mg_wakeup().
#define POLL_BACKSTOP_MS 250
#define COMMIT_WAIT_TIMEOUT_MS 3000
#define MAX_PENDING_WRITES 4096
// Arbitrary positive id for mg_wakeup(); doesn't need to name a real
// connection, just needs to be >0.
#define WAKEUP_CONN_ID 1

// A write's HTTP reply is deferred until its log entry commits, instead of
// blocking the connection thread and serialising all other requests.
typedef struct {
    struct mg_connection *conn;
    int targetIndex;
    int term;
    long long submittedAtMs;
} PendingWrite;

static PendingWrite pendingWrites[MAX_PENDING_WRITES];
static int numPendingWrites = 0;

// Lets notifyWriteProgress() (called from the raft thread) wake this node's
// event loop; atomic since it's written on one thread, read on others.
static _Atomic(struct mg_mgr *) activeMgr = NULL;

void notifyWriteProgress(void) {
    struct mg_mgr *mgr = atomic_load(&activeMgr);
    if (mgr != NULL) mg_wakeup(mgr, WAKEUP_CONN_ID, "", 0);
}

static long long nowMs(void) {
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return (long long)tv.tv_sec * 1000 + tv.tv_usec / 1000;
}

// Returns false if the pending-writes table is full, in which case the
// caller must reply to the client directly instead of deferring the reply.
static bool addPendingWrite(struct mg_connection *c, int targetIndex, int term) {
    if (numPendingWrites >= MAX_PENDING_WRITES) return false;
    pendingWrites[numPendingWrites++] =
        (PendingWrite){c, targetIndex, term, nowMs()};
    return true;
}

static void removePendingWriteAt(int i) {
    pendingWrites[i] = pendingWrites[--numPendingWrites];
}

static void purgePendingWritesForConnection(struct mg_connection *c) {
    for (int i = 0; i < numPendingWrites;) {
        if (pendingWrites[i].conn == c) {
            removePendingWriteAt(i);
        } else {
            i++;
        }
    }
}

static void flushPendingWrites(void) {
    acquireRaftNodeLock();
    int commitIndex = node->commitIndex;
    bool isLeader = node->state == LEADER;
    int currentTerm = node->currentTerm;
    int leaderId = node->leaderId;
    releaseRaftNodeLock();

    long long now = nowMs();
    for (int i = 0; i < numPendingWrites;) {
        PendingWrite *pw = &pendingWrites[i];
        if (pw->targetIndex <= commitIndex) {
            mg_http_reply(
                pw->conn, OK_RESPONSE_CODE, "",
                "{\"success\": \"The write operation was successful\"}");
            removePendingWriteAt(i);
        } else if (!isLeader || currentTerm != pw->term) {
            mg_http_reply(
                pw->conn, OK_RESPONSE_CODE, "",
                "{\"error\": \"Lost leadership before write committed\", "
                "\"leaderId\": %d}",
                leaderId);
            removePendingWriteAt(i);
        } else if (now - pw->submittedAtMs > COMMIT_WAIT_TIMEOUT_MS) {
            mg_http_reply(
                pw->conn, OK_RESPONSE_CODE, "",
                "{\"error\": \"Timed out waiting for write to commit\"}");
            removePendingWriteAt(i);
        } else {
            i++;
        }
    }
}

static void handleClientQueryRequest(struct mg_connection *c,
                                     struct mg_http_message *hm) {
    char body[hm->body.len + 1];
    memcpy(body, hm->body.buf, hm->body.len);
    body[hm->body.len] = '\0';

    Operation operation = parseOperationJson(body);
    if (operation != NULL) printOperation(operation);
    if (operation == NULL) {
        LOG("Invalid operation passed into");
        mg_http_reply(c, BAD_REQUEST_RESPONSE_CODE, "",
                      "{\"error\": \"Invalid operation passed in\"}");
    } else if (isWriteOperation(operation)) {
        int targetIndex = -1;
        int term = -1;
        int leaderId = handleClientRequest(operation, &targetIndex, &term);
        if (leaderId == REQUEST_ACCEPTED) {
            if (!addPendingWrite(c, targetIndex, term)) {
                mg_http_reply(
                    c, OK_RESPONSE_CODE, "",
                    "{\"error\": \"Too many pending writes, try again\"}");
            }
        } else {
            // Not the leader, so the raft log never took ownership.
            mg_http_reply(
                c, OK_RESPONSE_CODE, "",
                "{\"error\": \"This is a follower node\", \"leaderId\": %d}",
                leaderId);
            freeOperation(operation);
        }
    } else {
        QueryResult queryResult = executeOperation(operation);

        char *queryResultJsonString = "Not result returned";
        if (queryResult != NULL) {
            queryResultJsonString = queryResultStringify(queryResult);
        }

        mg_http_reply(c, OK_RESPONSE_CODE, "", "{\"success\": %s}",
                      queryResultJsonString);

        // Reads aren't stored in the raft log, so nothing else owns these.
        if (queryResult != NULL) {
            free(queryResultJsonString);
            freeQueryResult(queryResult);
        }
        freeOperation(operation);
    }
}

static void handleLeaderQueryRequest(struct mg_connection *c,
                                     struct mg_http_message *hm) {
    mg_http_reply(c, OK_RESPONSE_CODE, "", "%d", getLeaderId());
}

static void handler(struct mg_connection *c, int ev, void *ev_data) {
    if (ev == MG_EV_CLOSE) {
        purgePendingWritesForConnection(c);
        return;
    }
    if (ev != MG_EV_HTTP_MSG) return;

    struct mg_http_message *hm = (struct mg_http_message *)ev_data;

    if (mg_match(hm->uri, mg_str("/"), NULL) &&
        mg_strcmp(hm->method, mg_str("POST")) == 0) {
        handleClientQueryRequest(c, hm);
        return;
    }

    if (mg_match(hm->uri, mg_str("/leader"), NULL) &&
        mg_strcmp(hm->method, mg_str("GET")) == 0) {
        handleLeaderQueryRequest(c, hm);
        return;
    }

    if (!mg_match(hm->uri, mg_str("/"), NULL) &&
        !mg_match(hm->uri, mg_str("/leader"), NULL)) {
        mg_http_reply(c, NOT_FOUND_RESPONSE_CODE, "", "");
        return;
    }
    mg_http_reply(c, METHOD_NOT_ALLOWED_RESPONSE_CODE, "", "");
}

static void startServer(int port) {
    LOG("Starting client handling server on port %d", port);

    char listenAddr[32];
    snprintf(listenAddr, sizeof(listenAddr), "https://0.0.0.0:%d", port);

    struct mg_mgr mgr;
    mg_mgr_init(&mgr);
    mg_wakeup_init(&mgr);
    mg_http_listen(&mgr, listenAddr, handler, NULL);

    atomic_store(&activeMgr, &mgr);
    setCommitIndexListener(notifyWriteProgress);

    for (;;) {
        mg_mgr_poll(&mgr, POLL_BACKSTOP_MS);
        flushPendingWrites();
    }
}

void *runClientHandlingServer(void *arg) {
    int port = *(int *)arg;
    startServer(port);
    return NULL;
}
