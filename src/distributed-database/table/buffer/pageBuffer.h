#ifndef PAGEBUFFER_H
#define PAGEBUFFER_H

#include <stddef.h>
#include "../table.h"
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
extern Page loadPage(TableInfo tableInfo, Buffer buffer, size_t pageId);

/**
 *
 * @param page Page to be written to
 * @param offset Offset from start of page
 * @param src Memory source to copy from
 * @param n Number of bytes to copy
 */
extern void copyToPage(Page page, uint16_t offset, void *src, size_t n);

/**
 *
 * @param page Page to be copied from
 * @param offset Offset from start of page
 * @param dst Pointer to copy to
 * @param n Number of bytes to copy
 */
extern void copyFromPage(Page page, uint16_t offset, void *dst, size_t n);

/**
 *
 * @param page Page to read record from
 * @param offset Offset to record from start of page
 * @return Parsed record at given location
 */
extern Record readRecord(Page page, uint16_t offset);

/**
 *
 * @param page Page to set header of
 * @param numRecords Number of records
 * @param recordStart Offset to start of record
 * @param freeSpace Amount of free space in page
 */
extern void setPageHeader(Page page, uint16_t numRecords, uint16_t recordStart, uint16_t freeSpace);

#endif
