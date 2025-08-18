#ifndef LOG_ENTRY_H
#define LOG_ENTRY_H

#include "table/operation.h"

typedef struct LogEntry *LogEntry;
struct LogEntry {
    int term;
    int logIndex;
    Operation operation;
};

/**
 * Create a heap allocated log entry
 * @param term the term to create the entry with
 * @param logIndex the index of the entry
 * @param operation the operation that log entry is storing
 * @return the heap allocated log entry
 */
extern LogEntry createLogEntry(int term, int logIndex, Operation operation);

/**
 * Free the memory used by the heap allocated log entry
 * @param entry the log entry to free
 */
extern void freeLogEntry(LogEntry entry);

#endif  // LOG_ENTRY_H
