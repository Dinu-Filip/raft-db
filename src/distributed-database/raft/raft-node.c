#include "raft/raft-node.h"

#include <assert.h>
#include <pthread.h>
#include <stdlib.h>
#include <sys/time.h>

#include "log.h"
#include "raft-node.h"
#include "raft/log-table.h"
#include "raft/persistent-store.h"

#define TO_STR(x) #x

RaftNode node;
void initRaftNode(int id, int numNodes) {
    initFilePaths(id);
    node = malloc(sizeof(struct RaftNode));
    assert(node != NULL);
    node->id = id;
    node->leaderId = NULL_NODE_ID;
    node->votedFor = readStoredVotedFor();
    setRaftNodeState(FOLLOWER);
    node->currentTerm = readStoredCurrentTerm();
    node->log = createLogTable();
    node->commitIndex = readStoredCommitIndex();
    node->numVotes = 0;
    node->nextIndex = createIntList();
    node->matchIndex = createIntList();

    setInteractionTime();

    node->numNodes = numNodes;
    for (int i = 0; i < numNodes; i++) {
        intListInsert(node->nextIndex, intListLength(node->nextIndex), 0);
        intListInsert(node->matchIndex, intListLength(node->matchIndex), -1);
    }

    // Initialising raftNodeLock to be re-entrant
    pthread_mutexattr_init(&node->raftNodeLockAttr);
    pthread_mutexattr_settype(&node->raftNodeLockAttr, PTHREAD_MUTEX_RECURSIVE);
    pthread_mutex_init(&node->raftNodeLock, &node->raftNodeLockAttr);
}

void setInteractionTime() {
    acquireRaftNodeLock();
    gettimeofday(&node->lastInteractionTime, NULL);
    releaseRaftNodeLock();
}

void acquireRaftNodeLock() { pthread_mutex_lock(&node->raftNodeLock); }
void releaseRaftNodeLock() { pthread_mutex_unlock(&node->raftNodeLock); }

static void logRaftNodeStateChange(RaftNodeState newState) {
    switch (newState) {
        case FOLLOWER:
            LOG("Node state changed to FOLLOWER");
            break;
        case CANDIDATE:
            LOG("Node state changed to CANDIDATE");
            break;
        case LEADER:
            LOG("Node state changed to LEADER");
            break;
    }
}

void setRaftNodeState(RaftNodeState newState) {
    if (node->state != newState) {
        logRaftNodeStateChange(newState);
    }
    node->state = newState;
    storeNodeState(newState);
}

void setLeaderId(int leaderId) {
    if (leaderId != node->leaderId) {
        LOG("Node leaderId set to %d", leaderId);
        node->leaderId = leaderId;
    }
}

void setCommitIndex(int newCommitIndex) {
    acquireRaftNodeLock();
    // COMMIT INDICES MUST ONLY INCREASE
    if (newCommitIndex < node->commitIndex) {
        LOG("Try to set commitIndex: %d which is lower than own commitIndex "
            "of: %d. Returning early",
            newCommitIndex, node->commitIndex);
        releaseRaftNodeLock();
        return;
    }
    if (newCommitIndex > node->commitIndex)
        LOG("INCREASE COMMIT INDEX TO %d", newCommitIndex);
    for (int i = node->commitIndex + 1; i <= newCommitIndex; i++) {
        LOG("EXECUTING Operation at index %d", i);
        executeOperation(logTableGet(node->log, i)->operation);
        LOG("FINISHED EXECUTING Operation at index %d", i);
    }
    node->commitIndex = newCommitIndex;
    storeCommitIndex(newCommitIndex);
    releaseRaftNodeLock();
}

void setCurrentTerm(int currentTerm) {
    node->currentTerm = currentTerm;
    storeCurrentTerm(currentTerm);
}

void setVotedFor(int votedFor) {
    node->votedFor = votedFor;
    storeVotedFor(votedFor);
}

int getLeaderId(void) {
    acquireRaftNodeLock();
    int leaderId = node->leaderId;
    releaseRaftNodeLock();
    return leaderId;
}
