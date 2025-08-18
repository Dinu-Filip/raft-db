#include "send.h"

#include <assert.h>
#include <stdbool.h>
#include <stdlib.h>

#include "networking/msg.h"
#include "networking/rpc.h"
#include "raft/log-entry.h"

static Msg makeMsg(MsgType type) {
    Msg msg = malloc(sizeof(struct Msg));
    assert(msg != NULL);
    msg->type = type;
    return msg;
}

void sendRequestVote(int senderTerm, int lastLogIndex, int lastLogTerm) {
    Msg msg = makeMsg(REQUEST_VOTE);
    msg->data.requestVote.senderTerm = senderTerm;
    msg->data.requestVote.lastLogIndex = lastLogIndex;
    msg->data.requestVote.lastLogTerm = lastLogTerm;
    nodeSendAll(msg);
}

void sendRequestVoteResponse(int candidateId, int term, bool voteGranted) {
    Msg msg = makeMsg(REQUEST_VOTE_RESPONSE);
    msg->data.requestVoteResponse.term = term;
    msg->data.requestVoteResponse.voteGranted = voteGranted;
    nodeSend(candidateId, msg);
}

void sendAppendEntries(int followerId, int term, int prevLogIndex,
                       int prevLogTerm, int leaderCommit, int numEntries,
                       LogEntry *entries) {
    Msg msg = makeMsg(APPEND_ENTRIES);
    msg->data.appendEntries.term = term;
    msg->data.appendEntries.prevLogIndex = prevLogIndex;
    msg->data.appendEntries.prevLogTerm = prevLogTerm;
    msg->data.appendEntries.leaderCommit = leaderCommit;
    msg->data.appendEntries.numEntries = numEntries;
    msg->data.appendEntries.entries = entries;
    nodeSend(followerId, msg);
}

void sendAppendEntriesResponse(int leaderId, int prevLogIndex, int numEntries,
                               int term, bool success) {
    Msg msg = makeMsg(APPEND_ENTRIES_RESPONSE);
    msg->data.appendEntriesResponse.prevLogIndex = prevLogIndex;
    msg->data.appendEntriesResponse.numEntries = numEntries;
    msg->data.appendEntriesResponse.term = term;
    msg->data.appendEntriesResponse.success = success;
    nodeSend(leaderId, msg);
}
