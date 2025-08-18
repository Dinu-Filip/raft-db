#ifndef RAFT_CALLBACKS_H
#define RAFT_CALLBACKS_H

#include <stdbool.h>

#include "log-entry.h"
#include "table/operation.h"

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
 */
extern void handleAppendEntries(int leaderId, int term, int prevLogIndex,
                                int prevLogTerm, int leaderCommit,
                                int numEntries, LogEntry *entries);

/**
 * Handle a response from a follower node to append entries
 */
extern void handleAppendEntriesResponse(int followerId, int prevLogIndex,
                                        int numEntries, int term, bool success);

/**
 * Handles a request from a client. Read operations can be handled by any node.
 * Write operations must be handled by the leader. If the operation given
 * is a write and the node is not the leader, it will return the leader id.
 * @param operation the client operation
 * @return the node that the request needs to be sent to or null node id if the
 * current node has handled the request
 */
extern int handleClientRequest(Operation operation);

#endif  // RAFT_CALLBACKS_H
