#ifndef SEND_H
#define SEND_H

#include <stdbool.h>

#include "raft/log-entry.h"

/**
 * Send RequestVote request to all other nodes
 * @param senderTerm the sender term
 * @param lastLogIndex the last log index
 * @param lastLogTerm the last log term
 */
extern void sendRequestVote(int senderTerm, int lastLogIndex, int lastLogTerm);

/**
 * Send a vote to a candidate node
 * @param candidateId the id of the node to send the vote to
 * @param term the current term
 * @param voteGranted indicates if the vote has been granted
 */
extern void sendRequestVoteResponse(int candidateId, int term,
                                    bool voteGranted);

/**
 * Send AppendEntries to a follower node, doesn't edit any values in entries
 * @param followerId the follower node to send the message to
 * @param term the current term
 * @param prevLogIndex the previous log index
 * @param prevLogTerm the previous log term
 * @param leaderCommit
 * @param numEntries the number of entries
 * @param entries the array of entries
 */
extern void sendAppendEntries(int followerId, int term, int prevLogIndex,
                              int prevLogTerm, int leaderCommit, int numEntries,
                              LogEntry *entries);

/**
 * Send AppendEntries response from the follower to the leader
 * @param leaderId the id of the node to send the message to
 * @param prevLogIndex the previous log index
 * @param numEntries the number of entries sent
 * @param term the current term
 * @param success indicates if the operation was successful
 */
extern void sendAppendEntriesResponse(int leaderId, int prevLogIndex,
                                      int numEntries, int term, bool success);

#endif  // SEND_H
