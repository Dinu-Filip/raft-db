#include "raft/log-entry.h"

#include <stdlib.h>

#include "log-entry.h"

LogEntry createLogEntry(int term, int logIndex, Operation operation) {
    LogEntry logEntry = malloc(sizeof(struct LogEntry));
    logEntry->term = term;
    logEntry->logIndex = logIndex;
    logEntry->operation = operation;
    return logEntry;
}

void freeLogEntry(LogEntry entry) { free(entry); }
