#include "server.h"

#include <math.h>
#include <stdlib.h>
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
        int leaderId = handleClientRequest(operation);
        if (leaderId == NULL_NODE_ID) {
            mg_http_reply(
                c, OK_RESPONSE_CODE, "",
                "{\"success\": \"The write operation was successful\"}");
        } else {
            mg_http_reply(
                c, OK_RESPONSE_CODE, "",
                "{\"error\": \"This is a follower node\", \"leaderId\": %d}",
                leaderId);
        }
    } else {
        QueryResult queryResult = executeOperation(operation);

        char *queryResultJsonString = "Not result returned";
        if (queryResult != NULL) {
            queryResultJsonString = queryResultStringify(queryResult);
        }

        mg_http_reply(c, OK_RESPONSE_CODE, "", "{\"success\": %s}",
                      queryResultJsonString);
    }
}

static void handleLeaderQueryRequest(struct mg_connection *c,
                                     struct mg_http_message *hm) {
    mg_http_reply(c, OK_RESPONSE_CODE, "", "%d", getLeaderId());
}

static void handler(struct mg_connection *c, int ev, void *ev_data) {
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
    mg_http_listen(&mgr, listenAddr, handler, NULL);

    for (;;) mg_mgr_poll(&mgr, 1000);
}

void *runClientHandlingServer(void *arg) {
    int port = *(int *)arg;
    startServer(port);
    return NULL;
}
