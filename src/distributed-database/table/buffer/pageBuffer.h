#ifndef PAGEBUFFER_H
#define PAGEBUFFER_H

#include <stddef.h>
#include "../table.h"

typedef struct Buffer *Buffer;
typedef struct Frame Frame;

/**
 *
 * @return Buffer with clock set to 0 and empty frame pool initialised
 */
extern Buffer initialiseBuffer();

/**
 *
 * @param buffer Buffer to be freed
 */
extern void freeBuffer(Buffer buffer);

/**
 *
 * @param tableInfo Contains database file to load pages from
 * @param buffer Buffer pool of pages in use
 * @param pageId ID of page to be loaded
 * @return Page returned for use
 */
extern Frame *loadFrame(TableInfo tableInfo, Buffer buffer, size_t pageId);

/**
 *
 * @param frame Frame to be written to
 * @param offset Offset from start of page
 * @param src Memory source to copy from
 * @param n Number of bytes to copy
 */
extern void copyToFrame(Frame *frame, uint16_t offset, void *src, size_t n);

extern void copyFromFrame(Frame *frame, uint16_t offset, void *dst, size_t n);

#endif
