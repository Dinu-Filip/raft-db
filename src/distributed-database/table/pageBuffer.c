#include "pageBuffer.h"
#include <assert.h>
#include <stdbool.h>
#include "int-hashmap.h"

#define BUFFER_POOL_SIZE 64
#define MAX_USAGE 5
#define EVICT_USAGE 0
#define INITIAL_CLOCK 0
#define INITIAL_USAGE 1

struct Frame {
    size_t usageIdx;
    bool dirty;
    Page page;
};
typedef struct Frame Frame;

struct Buffer {
    Frame frames[BUFFER_POOL_SIZE];
    size_t clock;
    IntHashmap frameMap;
};

Frame *getFrame(Buffer buffer);
void loadPageToFrame(Page page, Frame *frame);

Buffer initialiseBuffer() {
    Buffer buffer = malloc(sizeof(struct Buffer));
    assert(buffer != NULL);

    buffer->clock = INITIAL_CLOCK;
    buffer->frameMap = createIntHashmap(NULL);
    assert(buffer->frameMap != NULL);

    return buffer;
}

void freeBuffer(Buffer buffer) {
    freeIntHashmap(buffer->frameMap);
    free(buffer);
}

Page loadPage(TableInfo tableInfo, Buffer buffer, size_t pageId) {
    Frame *frame = getIntHashmap(buffer->frameMap, pageId);

    if (frame == NULL) {
        // Cache miss
        Page page = getPage(tableInfo, pageId);
        Frame *newFrame = getFrame(buffer);
        loadPageToFrame(page, newFrame);
        addIntHashmap(buffer->frameMap, pageId, newFrame);
        return page;
    }

    if (frame->usageIdx < MAX_USAGE) frame->usageIdx++;

    return frame->page;
}

Frame *getFrame(Buffer buffer) {
    size_t *clock = &buffer->clock;

    while (buffer->frames[*clock].usageIdx != EVICT_USAGE) {
        buffer->frames[*clock].usageIdx--;
        *clock++;
        if (*clock == BUFFER_POOL_SIZE) {
            *clock = INITIAL_CLOCK;
        }
    }

    return &buffer->frames[*clock];
}

void loadPageToFrame(Page page, Frame *frame) {
    frame->usageIdx = INITIAL_USAGE;
    frame->page = page;
    frame->dirty = false;
}
