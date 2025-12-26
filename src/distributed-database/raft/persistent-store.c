#include "raft/persistent-store.h"

#include <assert.h>
#include <errno.h>
#include <fcntl.h>
#include <math.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <unistd.h>

#include "log.h"
#include "networking/rpc.h"
#include "raft/log-table.h"
#include "raft/raft-node.h"
#include "table/operations/operation.h"

#define FILE_PATH_BASE "./raft-db/"
#define CURRENT_TERM_FILE_NAME "/currentterm"
#define VOTED_FOR_FILE_NAME "/votedfor"
#define COMMIT_INDEX_FILE_NAME "/commitindex"
#define LOG_TABLE_FILE_NAME "/logtable"
#define NODE_STATE_FILE_NAME "/nodestate"

#define BUFFER_SIZE 128

#define BASE_TEN 10

#define DEFAULT_CURRENT_TERM 0
#define DEFAULT_COMMIT_INDEX -1

#define FILE_OPEN_ERROR -1
#define FILE_WRITE_ERROR -1

static char *currentTermFilePath;
static char *votedForFilePath;
static char *commitIndexFilePath;
static char *logTableFilePath;
static char *nodeStateFilePath;

void initFilePaths(int nodeId) {
    int nodeIdCharacters = nodeId == 0 ? 1 : (int)log10(nodeId) + 1;

    currentTermFilePath = malloc((strlen(FILE_PATH_BASE) + nodeIdCharacters +
                                  strlen(CURRENT_TERM_FILE_NAME)) *
                                     sizeof(char) +
                                 1);
    assert(currentTermFilePath != NULL);
    sprintf(currentTermFilePath, "%s%d%s", FILE_PATH_BASE, nodeId,
            CURRENT_TERM_FILE_NAME);
    votedForFilePath = malloc((strlen(FILE_PATH_BASE) + nodeIdCharacters +
                               strlen(VOTED_FOR_FILE_NAME)) *
                                  sizeof(char) +
                              1);
    assert(votedForFilePath != NULL);
    sprintf(votedForFilePath, "%s%d%s", FILE_PATH_BASE, nodeId,
            VOTED_FOR_FILE_NAME);
    commitIndexFilePath = malloc((strlen(FILE_PATH_BASE) + nodeIdCharacters +
                                  strlen(COMMIT_INDEX_FILE_NAME)) *
                                     sizeof(char) +
                                 1);
    assert(commitIndexFilePath != NULL);
    sprintf(commitIndexFilePath, "%s%d%s", FILE_PATH_BASE, nodeId,
            COMMIT_INDEX_FILE_NAME);
    logTableFilePath = malloc((strlen(FILE_PATH_BASE) + nodeIdCharacters +
                               strlen(LOG_TABLE_FILE_NAME)) *
                                  sizeof(char) +
                              1);
    assert(logTableFilePath);
    sprintf(logTableFilePath, "%s%d%s", FILE_PATH_BASE, nodeId,
            LOG_TABLE_FILE_NAME);
    nodeStateFilePath = malloc((strlen(FILE_PATH_BASE) + nodeIdCharacters +
                                strlen(NODE_STATE_FILE_NAME)) *
                                   sizeof(char) +
                               1);
    assert(nodeStateFilePath);
    sprintf(nodeStateFilePath, "%s%d%s", FILE_PATH_BASE, nodeId,
            NODE_STATE_FILE_NAME);
}

static void writeNumToFile(const char *filePath, const int num) {
    FILE *file = fopen(filePath, "wb");

    if (file == NULL) {
        LOG_PERROR("Failed to open file");
    }

    fwrite(&num, sizeof(num), 1, file);

    fclose(file);
}

void storeCurrentTerm(int currentTerm) {
    writeNumToFile(currentTermFilePath, currentTerm);
}

void storeVotedFor(int votedFor) { writeNumToFile(votedForFilePath, votedFor); }

void storeCommitIndex(int commitIndex) {
    writeNumToFile(commitIndexFilePath, commitIndex);
}

void storeNodeState(RaftNodeState state) {
    writeNumToFile(nodeStateFilePath, state);
}

static int readStoredCurrentNum(const char *filePath, const int defaultValue) {
    FILE *file = fopen(filePath, "rb");

    if (file == NULL && errno == ENOENT) {
        return defaultValue;
    }
    if (file == NULL) {
        LOG_PERROR("Failed to open file");
    }

    int output;
    fread(&output, sizeof(output), 1, file);

    bool eof = feof(file);
    fclose(file);

    return eof ? defaultValue : output;
}

int readStoredCurrentTerm(void) {
    return readStoredCurrentNum(currentTermFilePath, DEFAULT_CURRENT_TERM);
}

int readStoredVotedFor(void) {
    return readStoredCurrentNum(votedForFilePath, NULL_NODE_ID);
}

int readStoredCommitIndex(void) {
    return readStoredCurrentNum(commitIndexFilePath, DEFAULT_COMMIT_INDEX);
}

static LogEntry parseLogEntry(ReadBuff readBuff) {
    LogEntry logEntry = malloc(sizeof(struct LogEntry));
    assert(logEntry != NULL);

    int ptrsCapacity = DEFAULT_PTRS_ARRAY_CAPACITY;
    int ptrsSize = 0;
    void **ptrs = malloc(ptrsCapacity * sizeof(void *));

    LOG_ENTRY(PARSE, PARSE_STRING, PARSE_MALLOC, PARSE_FREE, logEntry);

    free(ptrs);

    return logEntry;
}

void loadLogTable(LogTable l) {
    int fd = open(logTableFilePath, O_RDONLY | O_CREAT);
    if (fd == FILE_OPEN_ERROR) {
        LOG_PERROR("Failed to open log table file");
    }

    ReadBuff readBuff = createReadBuff(fd);

    for (;;) {
        LogEntry logEntry = parseLogEntry(readBuff);
        if (logEntry == NULL) break;

        logTablePushDirect(l, logEntry);

        // Advance past the bytes storing the size of the log entry
        checkReadBuff(readBuff, sizeof(uint64_t));
        readBuff->buff += sizeof(uint64_t);
    }

    free(readBuff);
    close(fd);
}

static EncodeRes encodeLogEntry(LogEntry logEntry) {
    uint64_t size = 0;
    uint64_t capacity = DEFAULT_ENCODE_BUFFER_CAPACITY;

    uint8_t *buffBase = malloc(capacity);
    assert(buffBase != NULL);

    uint8_t *buff = buffBase;

    LOG_ENTRY(ENCODE, ENCODE_STRING, ENCODE_MALLOC, ENCODE_FREE, logEntry);

    EncodeRes res = malloc(sizeof(struct EncodeRes));
    assert(res != NULL);
    res->size = size;
    res->buff = buffBase;

    return res;
}

void pushLogTableStore(LogEntry logEntry) {
    EncodeRes res = encodeLogEntry(logEntry);

    int fd = open(logTableFilePath, O_WRONLY | O_APPEND);
    if (fd == FILE_OPEN_ERROR) {
        LOG_PERROR("Failed to open log table file");
    }

    write(fd, res->buff, res->size);
    write(fd, (uint8_t *)&res->size, sizeof(res->size));

    close(fd);
    free(res->buff);
    free(res);
}

void popLogTableStore(void) {
    int fd = open(logTableFilePath, O_RDWR);
    if (fd == FILE_OPEN_ERROR) {
        LOG_PERROR("Failed to open log table file");
    }

    long filesize = lseek(fd, 0, SEEK_END);
    assert(filesize >= sizeof(uint64_t));

    lseek(fd, -sizeof(uint64_t), SEEK_END);
    uint64_t size;
    long bytesRead = read(fd, &size, sizeof(size));
    assert(bytesRead == sizeof(size));

    off_t newSize = filesize - sizeof(size) - size;
    assert(newSize >= 0);

    ftruncate(fd, newSize);
}
