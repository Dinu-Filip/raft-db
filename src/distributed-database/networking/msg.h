#ifndef MSG_H
#define MSG_H

#include <assert.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "log.h"
#include "raft/log-entry.h"
#include "table/operations/operation.h"

#define DEFAULT_READ_BUFFER_CAPACITY 1024
// Should be equal to the largest non variable message
#define DEFAULT_ENCODE_BUFFER_CAPACITY 14
#define DEFAULT_PTRS_ARRAY_CAPACITY 8

typedef struct ReadBuff *ReadBuff;
struct ReadBuff {
    int fd;
    uint8_t *base;
    uint8_t *buff;
    long size;
    long capacity;
};

#define PARSE_CHECK(s)                                        \
    if (checkReadBuff(readBuff, s) == CHECK_READ_BUFF_FAIL) { \
        PARSE_FREE();                                         \
        return NULL;                                          \
    }
#define PARSE(v)                               \
    {                                          \
        PARSE_CHECK(sizeof(v));                \
        memcpy(&v, readBuff->buff, sizeof(v)); \
        readBuff->buff += sizeof(v);           \
    }
#define PARSE_STRING(v)                  \
    {                                    \
        uint16_t len;                    \
        PARSE(len);                      \
        PARSE_CHECK(len * sizeof(char)); \
        PARSE_MALLOC(char, v, len + 1);  \
        memcpy(v, readBuff->buff, len);  \
        v[len] = '\0';                   \
        readBuff->buff += len;           \
    }
#define PARSE_MALLOC(t, v, n)                                    \
    {                                                            \
        v = malloc(n * sizeof(t));                               \
        assert(v != NULL);                                       \
        if (ptrsSize == ptrsCapacity) {                          \
            ptrsCapacity *= 2;                                   \
            ptrs = realloc(ptrs, ptrsCapacity * sizeof(char *)); \
            assert(ptrs != NULL);                                \
        }                                                        \
        ptrs[ptrsSize++] = v;                                    \
    }
#define PARSE_FREE()                         \
    {                                        \
        for (int i = 0; i < ptrsSize; i++) { \
            free(ptrs[i]);                   \
        }                                    \
    }

#define ENCODE_CHECK(s)                             \
    {                                               \
        if (size + s > capacity) {                  \
            capacity *= 2;                          \
            buffBase = realloc(buffBase, capacity); \
            buff = buffBase + size;                 \
        }                                           \
        size += s;                                  \
    }
#define ENCODE(v)                    \
    {                                \
        ENCODE_CHECK(sizeof(v));     \
        memcpy(buff, &v, sizeof(v)); \
        buff += sizeof(v);           \
    }
#define ENCODE_STRING(v)                  \
    {                                     \
        uint16_t len = strlen(v);         \
        ENCODE(len);                      \
        ENCODE_CHECK(len * sizeof(char)); \
        memcpy(buff, v, len);             \
        buff += len;                      \
    }
#define ENCODE_MALLOC(t, v, n)  // Empty
#define ENCODE_FREE()           // Empty

#define FORMAT_CODE(v)            \
    _Generic(v,                   \
        int: "%d",                \
        float: "%f",              \
        unsigned char: "%u",      \
        unsigned short: "%hu",    \
        unsigned int: "%u",       \
        long unsigned int: "%lu", \
        _Bool: "%d",              \
        char *: "%s",             \
        const char *: "%s",       \
        default: "%u")
#define PRINT(v)               \
    printf("%s: ", #v);        \
    printf(FORMAT_CODE(v), v); \
    printf("\n");
#define PRINT_STRING(v) LOG("%s: \"%s\" (length: %lu)", #v, v, strlen(v));
#define PRINT_MALLOC(t, v, n)  // Empty
#define PRINT_FREE()           // Empty

#define IDENTIFY(PROC, PROCS, MALLOC, FREE, identify) PROC(identify.id);

#define REQUEST_VOTE(PROC, PROCS, MALLOC, FREE, requestVote) \
    PROC(requestVote.senderTerm);                            \
    PROC(requestVote.lastLogIndex);                          \
    PROC(requestVote.lastLogTerm);

#define REQUEST_VOTE_RESPONSE(PROC, PROCS, MALLOC, FREE, requestVoteResponse) \
    PROC(requestVoteResponse.term);                                           \
    PROC(requestVoteResponse.voteGranted);

#define QUERY_ATTRIBUTES(PROC, PROCS, MALLOC, FREE, queryAttributes) \
    PROC(queryAttributes->numAttributes);                            \
    MALLOC(char *, queryAttributes->attributes,                      \
           queryAttributes->numAttributes);                          \
    for (int j = 0; j < queryAttributes->numAttributes; j++) {       \
        PROCS(queryAttributes->attributes[j]);                       \
    }
#define OPERAND(PROC, PROCS, MALLOC, FREE, operand)        \
    PROC(operand->type);                                   \
    switch (operand->type) {                               \
        case INT: {                                        \
            PROC(operand->value.intOp);                    \
            break;                                         \
        }                                                  \
        case STR:                                          \
        case VARSTR: {                                     \
            PROCS(operand->value.strOp);                   \
            break;                                         \
        }                                                  \
        case FLOAT: {                                      \
            PROC(operand->value.floatOp);                  \
            break;                                         \
        }                                                  \
        case BOOL: {                                       \
            PROC(operand->value.boolOp);                   \
            break;                                         \
        }                                                  \
        default: {                                         \
            LOG("Invalid operand type %d", operand->type); \
            FREE();                                        \
            return NULL;                                   \
        }                                                  \
    }
#define QUERY_VALUES(PROC, PROCS, MALLOC, FREE, queryValues)        \
    PROC(queryValues->numValues);                                   \
    MALLOC(Operand, queryValues->values, queryValues->numValues);   \
    for (int j = 0; j < queryValues->numValues; j++) {              \
        MALLOC(struct Operand, queryValues->values[j], 1);          \
        OPERAND(PROC, PROCS, MALLOC, FREE, queryValues->values[j]); \
    }
#define CONDITION(PROC, PROCS, MALLOC, FREE, condition)                       \
    PROC(condition->type);                                                    \
    switch (condition->type) {                                                \
        case NOT: {                                                           \
            PROCS(condition->value.oneArg.op1);                               \
            break;                                                            \
        }                                                                     \
        case EQUALS:                                                          \
        case LESS_THAN:                                                       \
        case GREATER_THAN:                                                    \
        case LESS_EQUALS:                                                     \
        case GREATER_EQUALS:                                                  \
        case AND:                                                             \
        case OR: {                                                            \
            PROCS(condition->value.twoArg.op1);                               \
            MALLOC(struct Operand, condition->value.twoArg.op2, 1);           \
            OPERAND(PROC, PROCS, MALLOC, FREE, condition->value.twoArg.op2);  \
            break;                                                            \
        }                                                                     \
        case BETWEEN: {                                                       \
            PROCS(condition->value.between.op1);                              \
            MALLOC(struct Operand, condition->value.between.op2, 1);          \
            OPERAND(PROC, PROCS, MALLOC, FREE, condition->value.between.op2); \
            MALLOC(struct Operand, condition->value.between.op3, 1);          \
            OPERAND(PROC, PROCS, MALLOC, FREE, condition->value.between.op3); \
            break;                                                            \
        }                                                                     \
        default: {                                                            \
            LOG("Invalid condition type %d", condition->type);                \
            FREE();                                                           \
            return NULL;                                                      \
        }                                                                     \
    }
#define QUERY_TYPES(PROC, PROCS, MALLOC, FREE, queryTypes)    \
    PROC(queryTypes->numTypes);                               \
    MALLOC(uint8_t, queryTypes->types, queryTypes->numTypes); \
    for (int j = 0; j < queryTypes->numTypes; j++) {          \
        PROC(queryTypes->types[j]);                           \
    }                                                         \
    PROC(queryTypes->numSizes);                               \
    MALLOC(size_t, queryTypes->sizes, queryTypes->numSizes);  \
    for (int j = 0; j < queryTypes->numSizes; j++) {          \
        PROC(queryTypes->sizes[j]);                           \
    }
#define OPERATION(PROC, PROCS, MALLOC, FREE, operation)                        \
    PROCS(operation->tableName);                                               \
    PROC(operation->queryType);                                                \
    switch (operation->queryType) {                                            \
        case SELECT: {                                                         \
            MALLOC(struct QueryAttributes, operation->query.select.attributes, \
                   1);                                                         \
            QUERY_ATTRIBUTES(PROC, PROCS, MALLOC, FREE,                        \
                             operation->query.select.attributes);              \
            break;                                                             \
        }                                                                      \
        case INSERT: {                                                         \
            MALLOC(struct QueryAttributes, operation->query.insert.attributes, \
                   1);                                                         \
            QUERY_ATTRIBUTES(PROC, PROCS, MALLOC, FREE,                        \
                             operation->query.insert.attributes);              \
            MALLOC(struct QueryValues, operation->query.insert.values, 1);     \
            QUERY_VALUES(PROC, PROCS, MALLOC, FREE,                            \
                         operation->query.insert.values);                      \
            break;                                                             \
        }                                                                      \
        case UPDATE: {                                                         \
            MALLOC(struct QueryAttributes, operation->query.update.attributes, \
                   1);                                                         \
            QUERY_ATTRIBUTES(PROC, PROCS, MALLOC, FREE,                        \
                             operation->query.update.attributes);              \
            MALLOC(struct QueryValues, operation->query.update.values, 1);     \
            QUERY_VALUES(PROC, PROCS, MALLOC, FREE,                            \
                         operation->query.update.values);                      \
            MALLOC(struct Condition, operation->query.update.condition, 1);    \
            CONDITION(PROC, PROCS, MALLOC, FREE,                               \
                      operation->query.update.condition);                      \
            break;                                                             \
        }                                                                      \
        case DELETE: {                                                         \
            MALLOC(struct Condition, operation->query.delete.condition, 1);    \
            CONDITION(PROC, PROCS, MALLOC, FREE,                               \
                      operation->query.delete.condition);                      \
            break;                                                             \
        }                                                                      \
        case CREATE_TABLE: {                                                   \
            MALLOC(struct QueryAttributes,                                     \
                   operation->query.createTable.attributes, 1);                \
            QUERY_ATTRIBUTES(PROC, PROCS, MALLOC, FREE,                        \
                             operation->query.createTable.attributes);         \
            MALLOC(struct QueryTypes, operation->query.createTable.types, 1);  \
            QUERY_TYPES(PROC, PROCS, MALLOC, FREE,                             \
                        operation->query.createTable.types);                   \
            break;                                                             \
        }                                                                      \
        default: {                                                             \
            LOG("Invalid operation type %d", operation->queryType);            \
            FREE();                                                            \
            return NULL;                                                       \
        }                                                                      \
    }
#define LOG_ENTRY(PROC, PROCS, MALLOC, FREE, logEntry) \
    PROC(logEntry->term);                              \
    PROC(logEntry->logIndex);                          \
    MALLOC(struct Operation, logEntry->operation, 1);  \
    OPERATION(PROC, PROCS, MALLOC, FREE, logEntry->operation);
#define APPEND_ENTRIES(PROC, PROCS, MALLOC, FREE, appendEntries)        \
    PROC(appendEntries.term);                                           \
    PROC(appendEntries.prevLogIndex);                                   \
    PROC(appendEntries.prevLogTerm);                                    \
    PROC(appendEntries.leaderCommit);                                   \
    PROC(appendEntries.numEntries);                                     \
    MALLOC(LogEntry, appendEntries.entries, appendEntries.numEntries);  \
    for (int i = 0; i < appendEntries.numEntries; i++) {                \
        MALLOC(struct LogEntry, appendEntries.entries[i], 1);           \
        LOG_ENTRY(PROC, PROCS, MALLOC, FREE, appendEntries.entries[i]); \
    }

#define APPEND_ENTRIES_RESPONSE(PROC, PROCS, MALLOC, FREE, \
                                appendEntriesResponse)     \
    PROC(appendEntriesResponse.prevLogIndex);              \
    PROC(appendEntriesResponse.numEntries);                \
    PROC(appendEntriesResponse.term);                      \
    PROC(appendEntriesResponse.success);

#define MSG(PROC, PROCS, MALLOC, FREE, msg)                                 \
    PROC(msg->type);                                                        \
    switch (msg->type) {                                                    \
        case CLIENT_IDENTIFY:                                               \
        case SERVER_IDENTIFY: {                                             \
            IDENTIFY(PROC, PROCS, MALLOC, FREE, msg->data.identify);        \
            break;                                                          \
        }                                                                   \
        case PING: {                                                        \
            break;                                                          \
        }                                                                   \
        case REQUEST_VOTE: {                                                \
            REQUEST_VOTE(PROC, PROCS, MALLOC, FREE, msg->data.requestVote); \
            break;                                                          \
        }                                                                   \
        case REQUEST_VOTE_RESPONSE: {                                       \
            REQUEST_VOTE_RESPONSE(PROC, PROCS, MALLOC, FREE,                \
                                  msg->data.requestVoteResponse);           \
            break;                                                          \
        }                                                                   \
        case APPEND_ENTRIES: {                                              \
            APPEND_ENTRIES(PROC, PROCS, MALLOC, FREE,                       \
                           msg->data.appendEntries);                        \
            break;                                                          \
        }                                                                   \
        case APPEND_ENTRIES_RESPONSE: {                                     \
            APPEND_ENTRIES_RESPONSE(PROC, PROCS, MALLOC, FREE,              \
                                    msg->data.appendEntriesResponse);       \
            break;                                                          \
        }                                                                   \
        default: {                                                          \
            LOG("Invalid message type %d", msg->type);                      \
            FREE();                                                         \
            return NULL;                                                    \
        }                                                                   \
    }

typedef enum {
    CLIENT_IDENTIFY = 0,
    SERVER_IDENTIFY,
    PING,
    REQUEST_VOTE,
    REQUEST_VOTE_RESPONSE,
    APPEND_ENTRIES,
    APPEND_ENTRIES_RESPONSE,
} MsgType;

typedef struct Msg *Msg;
struct Msg {
    uint8_t type;
    union {
        struct {
            int id;
        } identify;
        struct {
            int senderTerm;
            int lastLogIndex;
            int lastLogTerm;
        } requestVote;
        struct {
            int term;
            bool voteGranted;
        } requestVoteResponse;
        struct {
            int term;
            int prevLogIndex;
            int prevLogTerm;
            int leaderCommit;
            int numEntries;
            LogEntry *entries;
        } appendEntries;
        struct {
            int prevLogIndex;
            int numEntries;
            int term;
            bool success;
        } appendEntriesResponse;
    } data;
};

/**
 * Free the given message and the log entries array
 * @param msg the Msg to free
 */
extern void freeMsgShallow(Msg msg);

/**
 * Create a read buff with a default size
 * @param fd the file descriptor of the connection
 */
extern ReadBuff createReadBuff(int fd);

/**
 * Free a ReadBuff
 * @param readBuff the ReadBuff to free
 */
extern void freeReadBuff(ReadBuff readBuff);

/**
 * Parse a binary message into the internal format
 * @param readBuff the read buffer to parse
 */
extern Msg parse(ReadBuff readBuff);

typedef struct EncodeRes *EncodeRes;
struct EncodeRes {
    uint64_t size;
    uint8_t *buff;
};

/**
 * Encode a messaage into binary
 * @param msg the message to encode
 */
extern EncodeRes encode(Msg msg);

/**
 * Prints a mesasge struct
 * @param msg the message to print
 */
extern void *printMsg(Msg msg);

/**
 * Prints an operation struct
 * @param operation the operation to print
 */
extern void *printOperation(Operation operation);

#endif  // MSG_H
