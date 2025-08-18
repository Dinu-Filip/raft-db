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

#endif  // RAFT_MAIN_H
