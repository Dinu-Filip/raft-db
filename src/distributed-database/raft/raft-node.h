#ifndef RAFT_NODE_H
#define RAFT_NODE_H

#include <pthread.h>
#include <sys/time.h>

#include "int-list.h"
#include "log-table.h"

#define NULL_NODE_ID (-1)

typedef enum { FOLLOWER, CANDIDATE, LEADER } RaftNodeState;

typedef struct RaftNode *RaftNode;
struct RaftNode {
    int id;
    int leaderId;
    int votedFor;
    RaftNodeState state;
    /**
     * The current term of the node, must be exchanged when two nodes
     * communicate, if the other's term is higher, currentTerm must be updated
     * and state set to FOLLOWER. If lower, the request must be rejected
     */
    int currentTerm;
    LogTable log;
    int commitIndex;
    int lastApplied;
    int numVotes;
    IntList nextIndex;
    IntList matchIndex;

    struct timeval lastInteractionTime;
    int numNodes;

    pthread_mutexattr_t raftNodeLockAttr;
    pthread_mutex_t raftNodeLock;
};

/**
 * The global raft node object
 */
extern RaftNode node;

/**
 * Initialise the global raft node with either default values or the ones stored
 * in persistent storage
 * @param id the id of the node itself
 * @param numNodes the number of nodes in the cluster
 */
extern void initRaftNode(int id, int numNodes);

/**
 * Called when the node is interacted with from the leader to reset the election
 * timeout
 */
extern void setInteractionTime(void);

/**
 * Acquire the lock before accessing or editing the global raft node
 */
extern void acquireRaftNodeLock(void);

/**
 * Release the lock after accessing or editing the global raft node
 */
extern void releaseRaftNodeLock(void);

/**
 * Set the raft state to either FOLLOWER, CANDIDATE or LEADER
 * @param newState the state to set the raft node to
 */
extern void setRaftNodeState(RaftNodeState newState);

/**
 * Set the leaderId for the node
 * @param leaderId the leader id to set in the raft node
 */
extern void setLeaderId(int leaderId);

/**
 * If `newCommitIndex >= node->commitIndex`, commit all log entries from old
 * commit index to new commit index, set commit index and store it in persistant
 * storage
 * @param newCommitIndex the commitIndex to set the node to
 */
extern void setCommitIndex(int newCommitIndex);

/**
 * Set the currentTerm for the node. Stores the currentTerm in persistent
 * storage
 * @param currentTerm the current term to set the node to
 */
extern void setCurrentTerm(int currentTerm);

/**
 * Set votedFor for the node. Stores votedFor in persistent storage
 * @param votedFor the node for which the current node voted for to become the
 * next leader
 */
extern void setVotedFor(int votedFor);

/**
 * Get the leader id
 * @return the leader id
 */
extern int getLeaderId(void);

#endif  // RAFT_NODE_H
