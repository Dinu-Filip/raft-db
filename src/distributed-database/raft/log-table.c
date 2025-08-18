#include "log-table.h"

#include <assert.h>

#include "log.h"
#include "persistent-store.h"
#include "raft/log-entry.h"

#define INITIAL_CAPACITY 128

struct LogTable {
    size_t length;
    size_t capacity;
    LogEntry *logEntries;
};

LogTable createLogTable() {
    LogTable l = malloc(sizeof(struct LogTable));
    assert(l != NULL);

    l->length = 0;
    l->capacity = INITIAL_CAPACITY;
    l->logEntries = malloc(sizeof(LogEntry) * l->capacity);
    assert(l->logEntries != NULL);

    loadLogTable(l);

    return l;
}

void freeLogTable(LogTable l) {
    for (int i = 0; i < l->length; i++) {
        freeLogEntry(l->logEntries[i]);
    }

    free(l->logEntries);
    free(l);
}

LogEntry logTableGet(LogTable l, size_t index) {
    if (index < 0 || index >= l->length) {
        return NULL;
    }

    return l->logEntries[index];
}

static void resize(LogTable l) {
    l->capacity *= 2;
    l->logEntries = realloc(l->logEntries, sizeof(LogEntry) * l->capacity);
    if (l->logEntries == NULL) {
        LOG_ERROR(
            "Could not increase size of LogTable. Tried to resize to: %ld",
            sizeof(LogEntry) * l->capacity);
    }
}

void logTablePushDirect(LogTable l, LogEntry entry) {
    assert(entry->logIndex == l->length);

    if (l->length >= l->capacity) {
        resize(l);
    }

    l->logEntries[l->length++] = entry;
}

void logTablePush(LogTable l, LogEntry entry) {
    logTablePushDirect(l, entry);
    pushLogTableStore(entry);
}

void logTablePop(LogTable l) {
    l->length--;
    freeLogEntry(l->logEntries[l->length]);
    popLogTableStore();
}

int logTableLength(LogTable l) { return l->length; }
