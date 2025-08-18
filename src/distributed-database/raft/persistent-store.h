#ifndef PERSISTENT_STORE_H
#define PERSISTENT_STORE_H

#include "log-table.h"
#include "raft/log-entry.h"
#include "raft/raft-node.h"

/**
 * Initialise the file paths for the specified node id
 * @param nodeId the node id to specify the persistent storage file paths for
 */
extern void initFilePaths(int nodeId);

/**
 * Write the currentTerm to persistent storage
 * @param currentTerm the current term to write to persistent storage
 */
extern void storeCurrentTerm(int currentTerm);

/**
 * Read the currentTerm from the store or default value if it can't be read
 * @return the value that is read from the store or the default value
 */
extern int readStoredCurrentTerm(void);

/**
 * Write the votedFor to persistent storage
 * @param votedFor the node id that was votedFor that is to be written to
 * persistent storage
 */
extern void storeVotedFor(int votedFor);

/**
 * Read the votedFor from the store or default value if it can't be read
 * @return the value that is read from the store or default value
 */
extern int readStoredVotedFor(void);

extern void storeNodeState(RaftNodeState state);

/**
 * Write the commitIndex to persistent storage
 * @param commitIndex the commit index to write to persistent storage
 */
extern void storeCommitIndex(int commitIndex);

/**
 * Read the commitIndex from the store or default value if it can't be read
 * @return the value that is read from the store or default value
 */
extern int readStoredCommitIndex(void);

/**
 * Initialise the passed in log table with the stored log table in persistent
 * storage
 * @param l the log table to initialise with the log entries in persistent
 * storage
 */
extern void loadLogTable(LogTable l);

/**
 * Push the log entry to the persistent storage
 * @param l the log entry to push to persistent storage
 */
extern void pushLogTableStore(LogEntry l);

/**
 * Pop the last log entry from persistent storage
 */
extern void popLogTableStore(void);

#endif  // PERSISTENT_STORE_H
