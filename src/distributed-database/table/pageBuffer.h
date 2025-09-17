#ifndef PAGEBUFFER_H
#define PAGEBUFFER_H
#include <stddef.h>

#include "pages.h"

typedef struct Buffer *Buffer;

extern Buffer initialiseBuffer();
extern void freeBuffer(Buffer buffer);
extern struct Page loadPage(size_t loadPage);

#endif
