#ifndef LOG_TABLE_H
#define LOG_TABLE_H

#include <stdlib.h>

#include "log-entry.h"

typedef struct LogTable *LogTable;

extern LogTable createLogTable(void);
extern void freeLogTable(LogTable l);

extern LogEntry logTableGet(LogTable l, size_t index);
extern void logTablePushDirect(LogTable l, LogEntry entry);
extern void logTablePush(LogTable l, LogEntry entry);
extern void logTablePop(LogTable l);
extern int logTableLength(LogTable l);

#endif  // LOG_TABLE_H
