#ifndef RAFT_CALLBACKS_H
#define RAFT_CALLBACKS_H

#include <stdbool.h>
#include <stdint.h>

#include "log-entry.h"
#include "table/operations/operation.h"

// Distinct from NULL_NODE_ID, which leaderId also holds when no leader is
// known yet - lets handleClientRequest's caller tell the two cases apart.
#define REQUEST_ACCEPTED (-2)

/**
 * Handle the request for a vote from the sender node
 * @param senderId the sender node's id
 * @param senderTerm the term of the sender node
 * @param senderLastLogIndex the last log index of the sender node
 * @param senderLastLogTerm the term of the last log of the sender node
 */
extern void handleRequestVote(int senderId, int senderTerm,
                              int senderLastLogIndex, int senderLastLogTerm);

/**
 * Handle the response from a vote request
 * @param voterId the id of the sender node
 * @param term the term of the sender node
 * @param voteGranted a bool that is true iff the sender node granted their vote
 * for the specified term to them
 */
extern void handleRequestVoteResponse(int voterId, int term, bool voteGranted);

/**
 * Handle a request from the leader to append entries to the node's log
 * @param sentAtNs the leader's monotonic clock reading at send time, echoed
 * back unchanged in the response for RPC latency measurement
 */
extern void handleAppendEntries(int leaderId, int term, int prevLogIndex,
                                int prevLogTerm, int leaderCommit,
                                uint64_t sentAtNs, int numEntries,
                                LogEntry *entries);

/**
 * Handle a response from a follower node to append entries
 * @param sentAtNs the sentAtNs value echoed back from the original request,
 * used to log the round-trip RPC latency on the leader
 * @param dequeuedAtNs monotonic time this response was popped off the
 * follower's job queue, before this function's lock acquisition - used to
 * split the logged RPC latency into queue/network time vs raft node lock
 * wait time
 */
extern void handleAppendEntriesResponse(int followerId, int prevLogIndex,
                                        int numEntries, int term,
                                        uint64_t sentAtNs,
                                        uint64_t dequeuedAtNs, bool success);

/**
 * Handles a request from a client. Read operations can be handled by any node.
 * Write operations must be handled by the leader. If the operation given
 * is a write and the node is not the leader, it will return the leader id.
 * If the node is the leader, outIndex/outTerm are set to the log index and
 * term the entry was appended at, so the caller can wait for it to commit.
 * @param operation the client operation
 * @param outIndex set to the appended entry's log index if this node is leader
 * @param outTerm set to the term the entry was appended in if this node is leader
 * @return REQUEST_ACCEPTED if this node handled it as leader, otherwise the
 * current known leader id, or NULL_NODE_ID if no leader is known yet
 */
extern int handleClientRequest(Operation operation, int *outIndex, int *outTerm);

#endif  // RAFT_CALLBACKS_H
