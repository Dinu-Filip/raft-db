#include "raft/raft.h"

#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <unistd.h>

#include "int-list.h"
#include "log.h"
#include "networking/send.h"
#include "raft/elections.h"
#include "raft/log-entry.h"
#include "raft/log-table.h"
#include "raft/raft-node.h"
#include "utils.h"

#define MAX_NUM_ENTRIES (1 << 8)

#define MAIN_THREAD_SLEEP_US 5000

void runAppendEntries(int followerId) {
    acquireRaftNodeLock();
    int prevLogIndex = intListGet(node->nextIndex, followerId) - 1;
    int numEntries =
        MIN(MAX_NUM_ENTRIES, logTableLength(node->log) - prevLogIndex - 1);
    LogEntry *entries =
        numEntries == 0 ? NULL : malloc(sizeof(LogEntry) * numEntries);
    for (int i = 0; i < numEntries; i++) {
        entries[i] = logTableGet(node->log, prevLogIndex + 1 + i);
    }
    int prevLogTerm =
        prevLogIndex == -1 ? 0 : logTableGet(node->log, prevLogIndex)->term;
    sendAppendEntries(followerId, node->currentTerm, prevLogIndex, prevLogTerm,
                      node->commitIndex, numEntries, entries);
    releaseRaftNodeLock();
}

void sendAllAppendEntries(void) {
    acquireRaftNodeLock();
    assert(node->state == LEADER);
    for (int i = 0; i < node->numNodes; i++) {
        if (i == node->id) continue;

        runAppendEntries(i);
    }
    releaseRaftNodeLock();
}

static void updateCommitIndex(void) {
    acquireRaftNodeLock();
    int l = -1;
    int r = 0;
    const int n = node->numNodes;
    for (int i = 0; i < n; i++) {
        r = MAX(r, intListGet(node->matchIndex, i));
    }
    while (l < r) {
        int mid = (l + r + 1) / 2;
        int geqCount = 0;
        for (int i = 0; i < n; i++) {
            if (intListGet(node->matchIndex, i) >= mid) {
                geqCount++;
            }
        }
        if (geqCount >= (n == 2 ? 1 : n / 2 + 1)) {
            l = mid;
        } else {
            r = mid - 1;
        }
    }
    if (l > node->commitIndex) {
        LOG("Update leader commit index to %d", l);
    }
    setCommitIndex(l);
    releaseRaftNodeLock();
}

static int primes[] = {3, 5, 7, 11, 13, 17};

static int modPow(int a, int b, int m) {
    if (b == 0) return 1;
    if (b == 1) return a % m;
    int sub = modPow(a, b / 2, m);
    sub *= sub;
    sub %= m;
    if (b % 2) sub = (sub * a) % m;
    return sub;
}

static void raftMain(void) {
    srand(time(NULL) * modPow(node->id, primes[node->id % 6], 10000));
    setElectionTimeout();
    for (;;) {
        acquireRaftNodeLock();
        if (node->state == CANDIDATE) {
            if (checkElectionWon()) {
                electionWon();
            }
        }

        if (shouldCallElection()) {
            commenceElection();
        }

        if (node->state == LEADER) {
            updateCommitIndex();
            sendAllAppendEntries();
        }
        releaseRaftNodeLock();
        usleep(MAIN_THREAD_SLEEP_US);
    }
}

void *runRaftMain(void *arg) {
    raftMain();
    return NULL;
}
