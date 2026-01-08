#include "buffer.h"

#include <assert.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "../lib/bitmap.h"
#include "int-list.h"
#include "table/core/table.h"

#define MAX_COUNT 5
#define NUM_PAGES 1000
#define BUFFER_SIZE (_PAGE_SIZE * NUM_PAGES)

typedef struct Frame Frame;
struct Frame {
    bool dirty;
    bool loaded;
    int file;
    uint16_t pageId;
    uint8_t cnt;
    uint8_t *ptr;
};

typedef struct Buffer *Buffer;
struct Buffer {
    void *ptr;
    IntList freeList;
    Frame frames[NUM_PAGES];
    uint16_t id;
};

Buffer initialiseBuffer() {
    Buffer buffer = malloc(sizeof(struct Buffer));
    assert(buffer != NULL);

    buffer->id = 0;
    int res = posix_memalign(&buffer->ptr, _PAGE_SIZE, BUFFER_SIZE);

    if (res != 0) {
        free(buffer);
        return NULL;
    }

    buffer->freeList = createIntList();
    for (int i = 0; i < NUM_PAGES; i++) {
        intListPush(buffer->freeList, i);
    }

    return buffer;
}


static incUsage(Frame *frame) {
    frame->cnt = frame->cnt == MAX_COUNT ? MAX_COUNT : frame->cnt + 1;
}

static decUsage(Frame *frame) {
    frame->cnt = frame->cnt == 0 ? 0 : frame->cnt - 1;
}

static void evict(Buffer buffer) {
    Frame *curr = &buffer->frames[buffer->id];

    while (curr->cnt > 0) {
        curr->cnt--;
        decUsage(curr);
        curr = &buffer->frames[buffer->id];
    }

    intListPush(buffer->freeList, buffer->id);
}

static size_t nextFreePage(Buffer buffer) {
    assert(buffer != NULL);

    if (intListLength(buffer->freeList) == 0) {
        evict(buffer);
    }

    size_t freeFrameId = intListPop(buffer->freeList);
    return freeFrameId;
}

Frame *allocFilePage(Buffer buffer, int fd, size_t pageId) {
    Frame *frame = &buffer->frames[nextFreePage(buffer)];

    frame->dirty = false;
    frame->loaded = false;
    frame->pageId = pageId;
    frame->cnt = 0;
    frame->file = fd;
    frame->ptr = buffer->ptr + pageId * _PAGE_SIZE;

    return frame;
}

static void loadFilePage(Frame *frame) {
    ssize_t res = pread(frame->file, frame->ptr, _PAGE_SIZE, frame->pageId * _PAGE_SIZE);
    assert(res != 0 && res != -1);
    frame->loaded = true;
}

void pgcpy(Frame *frame, uint16_t offset, void *dest, size_t size) {
    if (!frame->loaded) {
        loadFilePage(frame);
    }
    memcpy(dest, frame->ptr + offset, size);
    incUsage(frame);
}

void pgset(Frame *frame, uint16_t offset, void *src, size_t size) {
    if (!frame->loaded) {
        loadFilePage(frame);
    }
    memcpy(frame->ptr + offset, src, size);
    frame->dirty = true;
    incUsage(frame);
}

void freeFrame(Buffer buffer, Frame *frame) {
    intListPush(buffer->freeList, frame->pageId);
}
