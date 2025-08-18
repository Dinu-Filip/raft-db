#include "raft/elections.h"

#include <stdbool.h>
#include <stdlib.h>
#include <sys/time.h>

#include "int-list.h"
#include "log.h"
#include "networking/send.h"
#include "raft/log-entry.h"
#include "raft/log-table.h"
#include "raft/raft-node.h"
#include "raft/raft.h"

#define RANDOM_ELECTION_TIME_MIN 150000
#define RANDOM_ELECTION_TIME_RANGE 1000000

static struct timeval randomTime;

void setElectionTimeout(void) {
    int timeout =
        rand() % RANDOM_ELECTION_TIME_RANGE + RANDOM_ELECTION_TIME_MIN;
    randomTime.tv_sec = timeout / 1000000;
    randomTime.tv_usec = timeout % 1000000;
}

void commenceElection(void) {
    acquireRaftNodeLock();
    setInteractionTime();
    setCurrentTerm(node->currentTerm + 1);
    LOG("(commenceElection) Commencing election with new term of %d",
        node->currentTerm);
    setVotedFor(node->id);
    node->numVotes = 1;
    setRaftNodeState(CANDIDATE);
    setLeaderId(NULL_NODE_ID);
    int logLength = logTableLength(node->log);
    LogEntry lastEntry = logTableGet(node->log, logLength - 1);
    sendRequestVote(node->currentTerm, logLength - 1,
                    (lastEntry == NULL) ? 0 : lastEntry->term);
    releaseRaftNodeLock();
}

void electionWon(void) {
    LOG("(electionWon) Won election");
    acquireRaftNodeLock();
    setRaftNodeState(LEADER);
    setLeaderId(node->id);
    for (int i = 0; i < intListLength(node->nextIndex); i++) {
        intListSet(node->nextIndex, i, logTableLength(node->log));
    }
    sendAllAppendEntries();
    releaseRaftNodeLock();
}

bool checkElectionWon(void) {
    acquireRaftNodeLock();
    const int requiredVotes = node->numNodes == 2 ? 1 : node->numNodes / 2 + 1;
    const bool result = node->numVotes >= requiredVotes;
    releaseRaftNodeLock();
    return result;
}

bool shouldCallElection(void) {
    acquireRaftNodeLock();
    if (node->state == LEADER) {
        releaseRaftNodeLock();
        return false;
    }

    struct timeval time;
    gettimeofday(&time, NULL);

    timersub(&time, &randomTime, &time);

    const bool result = timercmp(&node->lastInteractionTime, &time, <=);
    releaseRaftNodeLock();
    return result;
}
