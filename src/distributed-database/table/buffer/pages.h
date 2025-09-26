#ifndef PAGES_H
#define PAGES_H

#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>

#include "pageBuffer.h"
#include "../table.h"

typedef struct SlotArray *SlotArray;

typedef struct PageHeader *PageHeader;
struct PageHeader {
    bool modified;
    uint16_t numRecords;
    uint16_t recordStart;
    uint16_t freeSpace;
    SlotArray recordSlots;
};

typedef struct Page *Page;
struct Page {
    uint8_t *ptr;
    PageHeader header;
    bool dirty;
    uint16_t pageId;
};

/**
 * Allocates and returns page from file at given index
 * @param table
 * @param pageId index of page
 * @return Page containing pointer to memory block and header
 */
extern Page getPage(TableInfo table, size_t pageId);

/**
 * Frees page
 * @param page
 */
extern void freePage(Page page);

/**
 * Returns page with smallest index that has enough free space to fit
 * record of size recordSize
 * @param tableInfo
 * @param spaceInfo space inventory
 * @param recordSize size of record to insert
 * @param tableType
 * @return Page containing pointer to memory block and page header
 */
extern Page nextFreePage(TableInfo tableInfo, TableInfo spaceInfo,
                         size_t recordSize, TableType tableType);

/**
 * Initialises fields of page header
 * @param page pointer to page
 */
extern void initialisePageHeader(uint8_t *page);

/**
 * Adds new page to database file
 * @param table table containing page
 * @return Page with pointer to memory block and header
 */
extern Page addPage(TableInfo table);

/**
 * Allocates and returns raw page with no header
 * @param table
 * @param pageSize
 * @param pageId index of page to return
 * @return pointer to start of page
 */
extern uint8_t *getRawPage(FILE *table, size_t pageSize, size_t pageId);

/**
 * Inserts entry for free space in page to space inventory
 * @param tableName name of table
 * @param spaceInfo space map
 * @param page page to add to space map
 */
extern void addPageToSpaceInventory(char *tableName, TableInfo spaceInfo,
                                    Page page);

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

/**
 *
 * @param tableInfo All table information
 * @param page Page to write to disk
 */
void updatePage(TableInfo tableInfo, Page page);

#endif  // PAGES_H
