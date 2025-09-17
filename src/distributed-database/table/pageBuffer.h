#ifndef PAGEBUFFER_H
#define PAGEBUFFER_H

#include <stddef.h>
#include "pages.h"

typedef struct Buffer *Buffer;

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

#endif
