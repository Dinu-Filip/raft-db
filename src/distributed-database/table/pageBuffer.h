#ifndef PAGEBUFFER_H
#define PAGEBUFFER_H

#include <stddef.h>
#include "pages.h"

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

/**
 *
 * @param frame frame with page header to modify
 * @param numRecords number of records in page
 * @param recordStart start of contiguous records
 * @param freeSpace amount of free space
 */
extern void setPageHeader(Frame *frame, uint16_t numRecords, uint16_t recordStart, uint16_t freeSpace);

/**
 *
 * @param frame frame to get page from
 * @return page stored in frame
 */
extern Page getPageFromFrame(Frame *frame);

extern PageHeader getPageHeader(Frame *frame);

#endif
