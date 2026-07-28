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

// Was 5000 (5ms). Log entry propagation and commit-index advancement are
// both already event-driven (sendAllAppendEntries is called directly from
// leaderHandleClientRequest on every write, and updateCommitIndex from
// handleAppendEntriesResponse on every majority-worthy response) - this tick
// only still matters for two things: sending heartbeats to keep followers
// from calling an election, and checking shouldCallElection(). Both only
// need to run comfortably faster than RANDOM_ELECTION_TIME_MIN (150ms,
// elections.c), so 5ms bought no correctness benefit here.
//
// It did cost real latency: every NetworkNode's outbound sends and inbound
// responses share one FIFO job queue drained by one worker thread per peer
// (networking/worker.c). At 5ms, the periodic heartbeat SEND job competes
// with AppendEntriesResponse EXECUTE jobs on that same queue often enough to
// produce a clearly bimodal round-trip latency: most RPCs land around
// 400-500us, but a large fraction (27-42% of samples in an idle 3-vs-5-node
// comparison) get stuck behind queued heartbeat traffic and land around
// 5.5-5.8ms instead - see bench/06-rpc-latency.sh. That fraction, and the
// tail it produces, shrank monotonically with this value in local testing
// (5ms: p90 ~5.6ms; 10ms: p90 ~0.5ms, p99 ~1.5ms; 20ms: p90 ~0.6ms, p99
// ~1ms) and 15ms was picked as the new value for the traditional ~10x
// margin under the election timeout floor while cutting that self-inflicted
// queueing contention.
#define MAIN_THREAD_SLEEP_US 15000

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
                      node->commitIndex, monotonicNs(), numEntries, entries);
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

void updateCommitIndex(void) {
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
