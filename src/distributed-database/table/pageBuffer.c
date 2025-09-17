#include "pageBuffer.h"

#include <stdbool.h>
#include "int-hashmap.h"

#define BUFFER_POOL_SIZE 64
#define MAX_USAGE 5

struct Frame {
    size_t pageId;
    size_t usageIdx;
    bool dirty;
    struct Page page;
};
typedef struct Frame Frame;

struct Buffer {
    Frame frames[BUFFER_POOL_SIZE];
    size_t clock;
    IntHashmap frameMap;
};

