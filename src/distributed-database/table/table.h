#ifndef DATA_DICTIONARY_H
#define DATA_DICTIONARY_H

#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>

#include "schema.h"

#define MAX_FILE_NAME_LEN 200
#define MAX_TABLE_NAME_LEN 100
#define DB_EXTENSION "rfdb"

#define _PAGE_SIZE 4096  // 4 KB
#define PAGE_SIZE_WIDTH 2

#define INT_WIDTH 4
#define STR_WIDTH 255
#define FLOAT_WIDTH 4
#define BOOL_WIDTH 1

#define RECORD_IDX 2
#define RECORD_HEADER_WIDTH 2

#define PAGE_SIZE_IDX 0
#define NUM_PAGES_IDX (PAGE_SIZE_IDX + PAGE_SIZE_WIDTH)
#define START_PAGE_IDX (NUM_PAGES_IDX + PAGE_ID_WIDTH)
#define GLOBAL_ID_IDX (START_PAGE_IDX + PAGE_ID_WIDTH)

#define PAGE_ID_WIDTH 4

#define INITIAL_RECORD_CAPACITY 1
#define TABLE_HEADER_PAGE 0
#define NUM_RECORDS_IDX 0
#define RECORD_START_IDX (NUM_RECORDS_IDX + NUM_RECORDS_WIDTH)
#define FREE_SPACE_IDX (RECORD_START_IDX + NUM_RECORDS_WIDTH)
#define POS_ARRAY_IDX (FREE_SPACE_IDX + NUM_RECORDS_WIDTH)

#define NUM_RECORDS_WIDTH 2

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

typedef enum { RELATION, SCHEMA, FREE_MAP } TableType;

typedef struct RecordSlot RecordSlot;
struct RecordSlot {
    bool modified;
    uint16_t offset;
    int16_t size;
    uint8_t *pos;
};

typedef struct AttributeInfo AttributeInfo;
struct AttributeInfo {
    char *name;
    AttributeType type;
    int size;
    bool primary;
};

typedef struct Field Field;
struct Field {
    char *attribute;
    AttributeType type;
    unsigned size;
    union {
        int32_t intValue;
        uint32_t uintValue;
        float floatValue;
        bool boolValue;
        char *stringValue;
    };
};

typedef struct Record *Record;
struct Record {
    Field *fields;
    unsigned int numValues;
    size_t size;         // Does not include record slot in page header
    uint32_t globalIdx;  // Global index of record
};

typedef struct TableHeader *TableHeader;
struct TableHeader {
    bool modified;
    size_t pageSize;
    size_t numPages;
    int startPage;
    uint32_t globalIdx;
};

typedef struct TableInfo *TableInfo;
struct TableInfo {
    FILE *table;
    TableHeader header;
    char *name;
};

typedef struct RecordArray *RecordArray;
struct RecordArray {
    Record *records;
    int size;
    int capacity;
};

typedef struct RecordIterator RecordIterator;
struct RecordIterator {
    size_t pageId;
    int slotIdx;
    Page page;
    RecordSlot *lastSlot;
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
 * Reads and parses table header
 * @param table table FILE pointer
 */
extern TableHeader getTableHeader(FILE *table);

/**
 * Creates resizing record array
 */
extern RecordArray createRecordArray();

/**
 * Frees resizing record array
 * @param records
 */
extern void freeRecordArray(RecordArray records);

/**
 * Adds record to resizing record array
 * @param records array of records
 * @param record record to add
 */
extern void addRecord(RecordArray records, Record record);

/**
 * Parses query into internal record representation
 * @param schema schema to parse
 * @param attributes attributes from query
 * @param values values from query
 * @param globalIdx global record index
 * @return Record containing array of fields
 */
extern Record parseQuery(Schema *schema, QueryAttributes attributes,
                         QueryValues values, uint32_t globalIdx);

/**
 * Reads raw bytes into record slot
 * @param slot pointer to slot
 * @param idx pointer to raw slot
 */
extern void getRecordSlot(RecordSlot *slot, uint8_t *idx);

/**
 * Shifts records and updates slots to ensure records are contiguous in memory
 * @param page page to read records from
 */
extern void defragmentRecords(Page page);

/**
 * Initialises fields of record iterator
 * @param iterator
 */
extern void initialiseRecordIterator(RecordIterator *iterator);

/**
 * Writes field to page
 * @param fieldStart pointer to start of field
 * @param field field to write
 */
extern void writeField(uint8_t *fieldStart, Field field);

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

extern int getStaticTypeWidth(AttributeType type);

/**
 * Compares slots in decreasing order of offset
 * @param slot1
 * @param slot2
 */
extern int compareSlots(const void *slot1, const void *slot2);

/**
 * Outputs fields of records
 * @param record
 */
extern void outputRecord(Record record);
extern void outputField(Field field, unsigned int rightPadding);

/**
 * Closes table file and frees tableInfo
 * @param tableInfo
 */
extern void closeTable(TableInfo tableInfo);

/**
 * Frees record iterator
 * @param iterator
 */
extern void freeRecordIterator(RecordIterator iterator);

/**
 * Frees record and fields
 * @param record
 */
extern void freeRecord(Record record);

/**
 * Frees attribute and value of field (for VARSTR and STR)
 * @param field
 */
extern void freeField(Field field);

#endif  // DATA_DICTIONARY_H
