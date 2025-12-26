#ifndef DATA_DICTIONARY_H
#define DATA_DICTIONARY_H

#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>

#include "../schema.h"

#define MAX_FILE_NAME_LEN 200
#define MAX_TABLE_NAME_LEN 100
#define DB_EXTENSION "rfdb"

#define _PAGE_SIZE 4096  // 4 KB
#define PAGE_SIZE_WIDTH 2

#define RECORD_HEADER_WIDTH 2

#define PAGE_SIZE_IDX 0
#define NUM_PAGES_IDX (PAGE_SIZE_IDX + PAGE_SIZE_WIDTH)
#define GLOBAL_ID_IDX (NUM_PAGES_IDX + PAGE_ID_WIDTH)

#define PAGE_ID_WIDTH 4

#define TABLE_HEADER_PAGE 0
#define NUM_SLOTS_IDX 0
#define RECORD_START_IDX (NUM_SLOTS_IDX + NUM_SLOTS_WIDTH)
#define FREE_SPACE_IDX (RECORD_START_IDX + NUM_SLOTS_WIDTH)
#define POS_ARRAY_IDX (FREE_SPACE_IDX + NUM_SLOTS_WIDTH)

#define NUM_SLOTS_WIDTH 2

#define PAGE_TAIL -1
#define OFFSET_WIDTH 2
#define SIZE_WIDTH 2

#define GLOBAL_ID_WIDTH 4
#define GLOBAL_ID_NAME "GLOBAL_IDX"
#define GLOBAL_ID_RECORD_IDX 0

#define DB_BASE_DIRECTORY "raft-db"

#define SLOT_SIZE (OFFSET_WIDTH + SIZE_WIDTH)

extern char DB_DIRECTORY[MAX_FILE_NAME_LEN];

typedef struct Page *Page;
typedef struct Record *Record;
typedef struct RecordIterator *RecordIterator;
typedef struct RecordArray *RecordArray;

typedef enum { RELATION, SCHEMA, FREE_MAP } TableType;

typedef struct TableHeader *TableHeader;
struct TableHeader {
    bool modified;
    size_t pageSize;
    size_t numPages;
    uint32_t globalIdx;
};

typedef struct TableInfo *TableInfo;
struct TableInfo {
    FILE *table;
    TableHeader header;
    char *name;
};

typedef struct QueryResult *QueryResult;
struct QueryResult {
    RecordArray records;
    int numRecords;
};

/**
 * Opens table and reads table header
 * @param tableName name of table to open
 * @return TableInfo containing FILE pointer and header
 */
extern TableInfo openTable(char *tableName);

/**
 * Creates new binary file for table and initialises its header
 * @param name name of the table to initialise
 */
extern void initialiseTable(char *name);

/**
 * Reads and parses table header
 * @param table table FILE pointer
 */
extern TableHeader getTableHeader(FILE *table);

/**
 * Shifts records and updates slots to ensure records are contiguous in memory
 * @param page page to read records from
 */
extern void defragmentRecords(Page page);

/**
 * Updates table header in page
 * @param tableInfo table to update
 */
extern void updateTableHeader(TableInfo tableInfo);

/**
 * Updates space inventory using page
 * @param tableName table with associate space map
 * @param spaceInventory space map
 * @param page page to update into space inventory
 */
extern void updateSpaceInventory(char *tableName, TableInfo spaceInventory,
                                 Page page);

/**
 * Compares slots in decreasing order of offset
 * @param slot1
 * @param slot2
 */
extern int compareSlots(const void *slot1, const void *slot2);

/**
 * Closes table file and frees tableInfo
 * @param tableInfo
 */
extern void closeTable(TableInfo tableInfo);

#endif  // DATA_DICTIONARY_H
