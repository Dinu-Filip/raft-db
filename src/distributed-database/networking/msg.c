#include "msg.h"

#include <assert.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>

#include "networking/rpc.h"
#include "raft/log-entry.h"
#include "table/operation.h"

void freeMsgShallow(Msg msg) {
    if (msg == NULL) return;
    if (msg->type == APPEND_ENTRIES &&
        msg->data.appendEntries.entries != NULL) {
        free(msg->data.appendEntries.entries);
    }
    free(msg);
}

ReadBuff createReadBuff(int fd) {
    ReadBuff readBuff = malloc(sizeof(struct ReadBuff));
    assert(readBuff != NULL);
    readBuff->fd = fd;
    readBuff->size = 0;
    readBuff->capacity = DEFAULT_READ_BUFFER_CAPACITY;
    readBuff->base = malloc(readBuff->capacity * sizeof(uint8_t));
    assert(readBuff->base != NULL);
    readBuff->buff = readBuff->base;
    return readBuff;
}

void freeReadBuff(ReadBuff readBuff) {
    free(readBuff->base);
    free(readBuff);
}

Msg parse(ReadBuff readBuff) {
    Msg msg = malloc(sizeof(struct Msg));
    assert(msg != NULL);

    int ptrsCapacity = DEFAULT_PTRS_ARRAY_CAPACITY;
    int ptrsSize = 0;
    void **ptrs = malloc(ptrsCapacity * sizeof(void *));
    assert(ptrs != NULL);
    ptrs[ptrsSize++] = msg;

    MSG(PARSE, PARSE_STRING, PARSE_MALLOC, PARSE_FREE, msg);

    free(ptrs);

    return msg;
}

EncodeRes encode(Msg msg) {
    uint64_t size = 0;
    uint64_t capacity = DEFAULT_ENCODE_BUFFER_CAPACITY;

    uint8_t *buffBase = malloc(capacity);
    assert(buffBase != NULL);

    uint8_t *buff = buffBase;

    MSG(ENCODE, ENCODE_STRING, ENCODE_MALLOC, ENCODE_FREE, msg);

    EncodeRes res = malloc(sizeof(struct EncodeRes));
    assert(res != NULL);
    res->size = size;
    res->buff = buffBase;

    return res;
}

void *printMsg(Msg msg) {
    MSG(PRINT, PRINT_STRING, PRINT_MALLOC, PRINT_FREE, msg);
    return NULL;
}

void *printOperation(Operation operation) {
    OPERATION(PRINT, PRINT_STRING, PRINT_MALLOC, PRINT_FREE, operation);
    return NULL;
}
