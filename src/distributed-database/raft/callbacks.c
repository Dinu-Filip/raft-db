#include "raft/callbacks.h"

#include <assert.h>
#include <stdbool.h>
#include <stddef.h>

#include "callbacks.h"
#include "int-list.h"
#include "log.h"
#include "networking/send.h"
#include "raft/log-entry.h"
#include "raft/log-table.h"
#include "raft/raft-node.h"
#include "raft/raft.h"
#include "utils.h"

static void checkTerm(int term) {
    acquireRaftNodeLock();
    if (term > node->currentTerm) {
        setCurrentTerm(term);
        setRaftNodeState(FOLLOWER);
        setVotedFor(NULL_NODE_ID);
    }
    releaseRaftNodeLock();
}

void handleRequestVote(int senderId, int senderTerm, int senderLastLogIndex,
                       int senderLastLogTerm) {
    acquireRaftNodeLock();
    checkTerm(senderTerm);
    const int lastLogIndex = logTableLength(node->log) - 1;
    const int lastLogTerm =
        lastLogIndex == -1 ? 0 : logTableGet(node->log, lastLogIndex)->term;
    const bool logAtLeastAsUpToDate = (senderLastLogTerm > lastLogTerm ||
                                       (senderLastLogTerm == lastLogTerm &&
                                        senderLastLogIndex >= lastLogIndex));
    bool grantVote = (senderTerm >= node->currentTerm) &&
                     logAtLeastAsUpToDate && (node->votedFor == NULL_NODE_ID);
    if (grantVote) {
        setVotedFor(senderId);
        setInteractionTime();
    }
    sendRequestVoteResponse(senderId, node->currentTerm, grantVote);
    releaseRaftNodeLock();
}

void handleRequestVoteResponse(int voterId, int term, bool voteGranted) {
    acquireRaftNodeLock();
    checkTerm(term);
    if (node->state != CANDIDATE) {
        releaseRaftNodeLock();
        return;
    }
    if (voteGranted) {
        node->numVotes++;
    }
    releaseRaftNodeLock();
}

void handleAppendEntries(int leaderId, int term, int prevLogIndex,
                         int prevLogTerm, int leaderCommit, uint64_t sentAtNs,
                         int numEntries, LogEntry *entries) {
    acquireRaftNodeLock();
    checkTerm(term);
    if (term < node->currentTerm) {
        sendAppendEntriesResponse(leaderId, prevLogIndex, numEntries,
                                  node->currentTerm, sentAtNs, false);
        releaseRaftNodeLock();
        return;
    }
    assert(term == node->currentTerm);
    assert(node->state != LEADER);
    if (node->state == CANDIDATE) {
        setRaftNodeState(FOLLOWER);
        setLeaderId(leaderId);
    }
    if (node->leaderId != leaderId) {
        assert(node->state == FOLLOWER);
        setLeaderId(leaderId);
    }
    setInteractionTime();
    if (prevLogIndex > -1) {
        if (prevLogIndex >= logTableLength(node->log)) {
            sendAppendEntriesResponse(leaderId, prevLogIndex, numEntries,
                                      node->currentTerm, sentAtNs, false);
            releaseRaftNodeLock();
            return;
        }
        const LogEntry ourPrevLogEntry = logTableGet(node->log, prevLogIndex);
        if (!(ourPrevLogEntry != NULL &&
              ourPrevLogEntry->term == prevLogTerm)) {
            sendAppendEntriesResponse(leaderId, prevLogIndex, numEntries,
                                      node->currentTerm, sentAtNs, false);
            releaseRaftNodeLock();
            return;
        }
    }

    // Iterates over entries to append. If an existing entry conflicts with a
    // new one delete the existing entry and all that follow it
    int numEntriesToPop = 0;
    int addIndex = 0;
    for (; addIndex < numEntries; addIndex++) {
        const int tableIndex = prevLogIndex + 1 + addIndex;
        LogEntry entry = logTableGet(node->log, tableIndex);
        if (entry == NULL) {
            // No entry at that index, we can freely append the rest
            break;
        }
        assert(entry->logIndex == tableIndex);
        if (entry->term != entries[addIndex]->term) {
            // Conflicting entry found, pop all entries in the log from the
            // existing entry onwards then add the rest of appendEntries
            numEntriesToPop = logTableLength(node->log) - tableIndex;
            break;
        }
    }
    for (int i = 0; i < numEntriesToPop; i++) {
        // MUST NOT DELETE LOG ENTRIES THAT WERE COMMITTED
        assert(logTableLength(node->log) - 1 < node->commitIndex);
        logTablePop(node->log);
    }
    for (; addIndex < numEntries; addIndex++) {
        logTablePush(node->log, entries[addIndex]);
    }

    if (leaderCommit > node->commitIndex) {
        const int indexOfLastNewEntry = prevLogIndex + numEntries;
        setCommitIndex(MIN(leaderCommit, indexOfLastNewEntry));
    }

    sendAppendEntriesResponse(leaderId, prevLogIndex, numEntries,
                              node->currentTerm, sentAtNs, true);
    if (numEntries > 0) {
        LOG("Finished successfully AppendEntries of non-zero entries");
    }
    releaseRaftNodeLock();
}

void handleAppendEntriesResponse(int followerId, int prevLogIndex,
                                 int numEntries, int term, uint64_t sentAtNs,
                                 uint64_t dequeuedAtNs, bool success) {
    acquireRaftNodeLock();
    uint64_t lockedAtNs = monotonicNs();
    checkTerm(term);
    // queueUs: time from send to being popped off this peer's job queue -
    // network transit, follower-side processing/response, and this leader's
    // own per-peer queue wait, all bundled together. lockWaitUs: time from
    // dequeue to actually acquiring raftNodeLock here - isolates leader-side
    // lock contention from everything upstream of it.
    double latencyUs = (lockedAtNs - sentAtNs) / 1000.0;
    double queueUs = (dequeuedAtNs - sentAtNs) / 1000.0;
    double lockWaitUs = (lockedAtNs - dequeuedAtNs) / 1000.0;
    LOG("RPC_LATENCY peer=%d rpc=append_entries numEntries=%d latencyUs=%.3f "
        "queueUs=%.3f lockWaitUs=%.3f",
        followerId, numEntries, latencyUs, queueUs, lockWaitUs);
    if (node->state != LEADER) {
        releaseRaftNodeLock();
        return;
    }
    if (success) {
        int newMatchIndex = MAX(prevLogIndex + numEntries,
                                intListGet(node->matchIndex, followerId));
        intListSet(node->matchIndex, followerId, newMatchIndex);
        intListSet(node->nextIndex, followerId, newMatchIndex + 1);
        updateCommitIndex();
    } else {
        intListSet(node->nextIndex, followerId, MAX(prevLogIndex - 1, 0));
        runAppendEntries(followerId);
    }
    releaseRaftNodeLock();
}

static void leaderHandleClientRequest(Operation operation) {
    LogEntry entry =
        createLogEntry(node->currentTerm, logTableLength(node->log), operation);
    LOG("Pushing operation at index %d to leader log", entry->logIndex);
    logTablePush(node->log, entry);
    intListSet(node->matchIndex, node->id, entry->logIndex);
    sendAllAppendEntries();
}

int handleClientRequest(Operation operation, int *outIndex, int *outTerm) {
    acquireRaftNodeLock();
    int res = REQUEST_ACCEPTED;
    if (node->state == LEADER) {
        *outIndex = logTableLength(node->log);
        *outTerm = node->currentTerm;
        leaderHandleClientRequest(operation);
    } else {
        res = node->leaderId;
    }
    releaseRaftNodeLock();
    return res;
}
