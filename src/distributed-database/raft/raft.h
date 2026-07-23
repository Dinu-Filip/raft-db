#ifndef RAFT_MAIN_H
#define RAFT_MAIN_H

/**
 * Calls appendEntries send functions with correct parameters
 * @param followerId the follower node's id to send the append entries to
 */
extern void runAppendEntries(int followerId);

/**
 * The main thread function for raft
 * Does not accept parameters in or return anything
 */
extern void *runRaftMain(void *arg);

/**
 * Call runAppendEntries for every node in the cluster except itself
 */
extern void sendAllAppendEntries(void);

/**
 * Recompute the leader's commit index from matchIndex, advancing (and
 * applying) it if a majority of the cluster now agrees on a higher index.
 */
extern void updateCommitIndex(void);

#endif  // RAFT_MAIN_H
