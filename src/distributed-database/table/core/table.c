#include "table.h"

#include <assert.h>
#include <limits.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "../db-utils.h"
#include "../schema.h"
#include "../operations/update.h"
#include "log.h"
#include "pages.h"
#include "../operations/operation.h"

#define INITIAL_NUM_PAGES 0
#define INITIAL_START_PAGE (-1)
#define INITIAL_GLOBAL_IDX 0

// Global database directory
char DB_DIRECTORY[MAX_FILE_NAME_LEN] = {'\0'};

static void initialiseHeader(FILE *headerptr) {
    LOG("Initialise header");
    fseek(headerptr, PAGE_SIZE_IDX, SEEK_SET);

    const int pageSize = _PAGE_SIZE;
    const int initialNumPages = INITIAL_NUM_PAGES;
    const int initialGlobalIdx = INITIAL_GLOBAL_IDX;

    fwrite(&pageSize, sizeof(uint8_t), PAGE_SIZE_WIDTH, headerptr);
    fwrite(&initialNumPages, sizeof(uint8_t), PAGE_ID_WIDTH, headerptr);
    fwrite(&initialGlobalIdx, sizeof(uint8_t), GLOBAL_ID_WIDTH, headerptr);

    fseek(headerptr, 0, SEEK_SET);
}

void initialiseTable(char *name) {
    char tableFile[MAX_FILE_NAME_LEN + MAX_TABLE_NAME_LEN];
    snprintf(tableFile, MAX_FILE_NAME_LEN + MAX_TABLE_NAME_LEN, "%s/%s.%s",
             DB_BASE_DIRECTORY, name, DB_EXTENSION);
    LOG("Initialise table %s\n", name);

    FILE *table = fopen(tableFile, "wb+");
    assert(table != NULL);

    initialiseHeader(table);
    fclose(table);
}

void freeTable(TableInfo tableInfo) {
    fclose(tableInfo->table);
    free(tableInfo->header);
    free(tableInfo);
}

TableInfo openTable(char *tableName) {
    char tableFile[MAX_FILE_NAME_LEN + MAX_TABLE_NAME_LEN];
    snprintf(tableFile, MAX_FILE_NAME_LEN + MAX_TABLE_NAME_LEN, "%s/%s.%s",
             DB_BASE_DIRECTORY, tableName, DB_EXTENSION);
    LOG("Open %s\n", tableFile);

    FILE *table = fopen(tableFile, "rb+");
    assert(table != NULL);

    TableInfo tableInfo = malloc(sizeof(struct TableInfo));
    assert(tableInfo != NULL);

    tableInfo->table = table;
    tableInfo->name = strdup(tableName);
    tableInfo->header = getTableHeader(table);

    LOG("Got table\n");
    return tableInfo;
}

int compareSlots(const void *slot1, const void *slot2) {
    // Compares slots in decreasing order of offset

    RecordSlot *s1 = (RecordSlot *)slot1;
    RecordSlot *s2 = (RecordSlot *)slot2;
    
    if (s1->offset == s2->offset) {
        return 0;
    }
    if (s1->offset < s2->offset) {
        return 1;
    }
    return -1;
}

TableHeader getTableHeader(FILE *table) {
    LOG("GET HEADER\n");

    TableHeader header = malloc(sizeof(struct TableHeader));
    assert(header != NULL);

    uint8_t *headerPage = getRawPage(table, _PAGE_SIZE, TABLE_HEADER_PAGE);

    memcpy(&header->pageSize, headerPage + PAGE_SIZE_IDX, PAGE_SIZE_WIDTH);
    memcpy(&header->numPages, headerPage + NUM_PAGES_IDX, PAGE_ID_WIDTH);
    memcpy(&header->globalIdx, headerPage + GLOBAL_ID_IDX, GLOBAL_ID_WIDTH);

    header->modified = false;

    free(headerPage);

    return header;
}

void freeTableHeader(TableHeader tableHeader) { free(tableHeader); }

void modifyRecordOffsets(Page page, size_t recordStart, size_t diff) {
    assert(diff >= 0);

    // Shifts offset to start of static fields
    uint16_t oldStaticFieldStart = 0;
    memcpy(&oldStaticFieldStart, page->ptr + recordStart, RECORD_HEADER_WIDTH);
    uint16_t newStaticFieldStart = oldStaticFieldStart + diff;
    memcpy(page->ptr + recordStart, &newStaticFieldStart, RECORD_HEADER_WIDTH);

    // Updates offset for each of the variable field slots
    size_t slotsStart = recordStart + RECORD_HEADER_WIDTH;
    while (slotsStart != oldStaticFieldStart) {
        uint16_t oldOffset = 0;
        memcpy(&oldOffset, page->ptr + slotsStart, OFFSET_WIDTH);
        uint16_t newOffset = oldOffset + diff;
        memcpy(page->ptr + slotsStart, &newOffset, OFFSET_WIDTH);
        slotsStart += SLOT_SIZE;
    }
}

void defragmentRecords(Page page) {
    int numSlots = page->header->slots.size;

    // Sorts slots in decreasing order of offset
    // This allows records to be shifted as far right as possible without
    // overwriting existing data
    RecordSlot **slots = malloc(sizeof(RecordSlot *) * numSlots);
    assert(slots != NULL);

    for (int i = 0; i < numSlots; i++) {
        slots[i] = &page->header->slots.slots[i];
    }

    qsort(slots, numSlots, sizeof(RecordSlot *), &compareSlots);

    size_t recordStart = _PAGE_SIZE;
    for (int i = 0; i < numSlots; i++) {
        RecordSlot *slot = slots[i];
        // Calculates expected offset of record from end of page using record
        // size

        // Shifts record if size of record is less than offset to next record
        // i.e. there is a gap between the pair of records
        if (slot->size != 0 && slot->size != PAGE_TAIL &&
            slot->offset != recordStart) {
            recordStart -= slot->size;

            size_t diff = recordStart - slot->offset;
            modifyRecordOffsets(page, slot->offset, diff);
            memmove(page->ptr + recordStart, page->ptr + slot->offset,
                    sizeof(uint8_t) * slot->size);

            slot->modified = true;
            slot->offset = recordStart;
        }
    }
    page->header->recordStart = recordStart;
    page->header->modified = true;

    free(slots);
}

void updateTableHeader(TableInfo tableInfo) {
    LOG("Update table header\n");

    TableHeader header = tableInfo->header;

    if (header->modified) {
        fseek(tableInfo->table, 0, SEEK_SET);
        fwrite(&header->pageSize, sizeof(uint8_t), PAGE_SIZE_WIDTH,
               tableInfo->table);
        fwrite(&header->numPages, sizeof(uint8_t), PAGE_ID_WIDTH,
               tableInfo->table);
        fwrite(&header->globalIdx, sizeof(uint8_t), GLOBAL_ID_WIDTH,
               tableInfo->table);
        fseek(tableInfo->table, 0, SEEK_SET);
    }
}

void updateSpaceInventory(char *tableName, TableInfo spaceInventory,
                          Page page) {
    LOG("Update space inventory\n");

    Condition cond = malloc(sizeof(struct Condition));
    assert(cond != NULL);

    cond->type = EQUALS;
    cond->value.twoArg.op1 = SPACE_INFO_ID;

    int id = page->pageId;
    assert(id <= INT_MAX);
    cond->value.twoArg.op2 = createOperand(INT, &id);

    int freeSpace = page->header->freeSpace;

    LOG("Free space: %d\n", freeSpace);
    assert(freeSpace <= INT_MAX);

    Schema *spaceInfoSchema = initSpaceInfoSchema(tableName);
    QueryAttributes updateSpaceAttributes =
        createQueryAttributes(1, SPACE_INFO_FREE_SPACE);
    QueryValues updateSpaceValues =
        createQueryValues(1, createOperand(INT, &freeSpace));
    QueryValues freeSpaceQueryValues =
        createQueryValues(1, createOperand(INT, &freeSpace));

    // Updates free space of page in space inventory
    updateTable(spaceInventory, NULL, updateSpaceAttributes,
                freeSpaceQueryValues, cond, spaceInfoSchema);
    extendedDisplayTable(tableName, FREE_MAP);

    free(cond->value.twoArg.op2);
    free(cond);
    free(spaceInfoSchema);
    freeQueryAttributes(updateSpaceAttributes);
    freeQueryValues(updateSpaceValues);
    freeQueryValues(freeSpaceQueryValues);
}

void closeTable(TableInfo tableInfo) {
    fclose(tableInfo->table);
    free(tableInfo->header);
    free(tableInfo->name);
    free(tableInfo);
}

