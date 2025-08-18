#include "pages.h"

#include <assert.h>
#include <limits.h>
#include <stdlib.h>
#include <string.h>

#include "insert.h"
#include "log.h"
#include "schema.h"
#include "select.h"
#include "table/operation.h"

#define INITIAL_NUM_SLOTS 10

static PageHeader getPageHeader(uint8_t *ptr) {
    PageHeader pageHeader = calloc(1, sizeof(struct PageHeader));
    assert(pageHeader != NULL);

    memcpy(&pageHeader->numRecords, ptr + NUM_RECORDS_IDX, NUM_RECORDS_WIDTH);
    memcpy(&pageHeader->recordStart, ptr + RECORD_START_IDX, NUM_RECORDS_WIDTH);
    memcpy(&pageHeader->freeSpace, ptr + FREE_SPACE_IDX, NUM_RECORDS_WIDTH);

    pageHeader->modified = false;

    int numSlots = INITIAL_NUM_SLOTS;
    RecordSlot *slots = malloc(sizeof(RecordSlot) * numSlots);
    assert(slots != NULL);

    RecordSlot slot;
    uint8_t *start = ptr + POS_ARRAY_IDX;

    // Iterates through record slots and stopping at the
    // terminating slot
    int idx = 0;
    do {
        if (idx == numSlots - 1) {
            numSlots += 10;

            // Dynamically resizes when end of record slots array reached
            RecordSlot *newSlots =
                realloc(slots, numSlots * sizeof(RecordSlot));
            assert(newSlots != NULL);

            slots = newSlots;
        }
        getRecordSlot(&slot, start);
        slot.modified = false;
        start += SLOT_SIZE;
        slots[idx++] = slot;
    } while (slot.size != PAGE_TAIL);

    pageHeader->recordSlots = slots;

    return pageHeader;
}

uint8_t *getRawPage(FILE *table, size_t pageSize, size_t pageId) {
    uint8_t *ptr = malloc(sizeof(uint8_t) * pageSize);
    assert(ptr != NULL);

    size_t pageStart = pageId * pageSize;
    assert(pageStart <= LONG_MAX);

    fseek(table, pageStart, SEEK_SET);
    fread(ptr, sizeof(uint8_t), pageSize, table);
    fseek(table, 0, SEEK_SET);

    LOG("GET RAW PAGE %ld\n", pageId);
    return ptr;
}

Page getPage(TableInfo table, size_t pageId) {
    const size_t pageSize = table->header->pageSize;

    Page page = malloc(sizeof(struct Page));
    assert(page != NULL);

    uint8_t *ptr = calloc(pageSize, sizeof(uint8_t));
    assert(ptr != NULL);

    assert(pageId < LONG_MAX);

    fseek(table->table, pageId * pageSize, SEEK_SET);
    fread(ptr, sizeof(uint8_t), pageSize, table->table);

    page->ptr = ptr;
    page->header = getPageHeader(ptr);
    page->pageId = (uint16_t)pageId;

    fseek(table->table, 0, SEEK_SET);
    return page;
}

Page addPage(TableInfo table) {
    LOG("ADD PAGE TO %s\n", table->name);

    size_t pageSize = table->header->pageSize;

    Page page = malloc(sizeof(struct Page));
    assert(page != NULL);

    uint8_t *ptr = malloc(sizeof(uint8_t) * pageSize);
    assert(ptr != NULL);

    initialisePageHeader(ptr);
    table->header->numPages++;

    size_t pageStart = table->header->numPages * pageSize;
    assert(pageStart <= LONG_MAX);

    fseek(table->table, table->header->numPages * pageSize, SEEK_SET);
    fread(ptr, sizeof(uint8_t), pageSize, table->table);

    page->ptr = ptr;
    page->header = getPageHeader(ptr);
    page->pageId = table->header->numPages;

    // Sets start page if no other pages allocated
    if (table->header->numPages == 1) {
        table->header->startPage = 1;
    };

    table->header->modified = true;

    fseek(table->table, 0, SEEK_SET);

    return page;
}

void addPageToSpaceInventory(char *tableName, TableInfo spaceInfo, Page page) {
    Operation operation = createInsertOperation();
    operation->tableName = spaceInfo->name;
    operation->query.insert.attributes = createQueryAttributes(
        3, SPACE_INFO_RELATION, SPACE_INFO_ID, SPACE_INFO_FREE_SPACE);
    operation->query.insert.values =
        createQueryValues(3, tableName, page->pageId, page->header->freeSpace);

    executeOperation(operation);
    freeInsertOperationTest(operation);
}

static void writePageHeader(Page page) {
    LOG("Update page header");
    PageHeader header = page->header;
    uint8_t *ptr = page->ptr;

    if (header->modified) {
        memcpy(ptr + NUM_RECORDS_IDX, &header->numRecords, NUM_RECORDS_WIDTH);
        memcpy(ptr + RECORD_START_IDX, &header->recordStart, NUM_RECORDS_WIDTH);
        memcpy(ptr + FREE_SPACE_IDX, &header->freeSpace, NUM_RECORDS_WIDTH);
    }

    // Updates modified slots
    int idx = 0;
    RecordSlot currentSlot;
    do {
        currentSlot = header->recordSlots[idx];
        if (currentSlot.modified) {
            memcpy(ptr + POS_ARRAY_IDX + idx * (OFFSET_WIDTH + SIZE_WIDTH),
                   &currentSlot.offset, OFFSET_WIDTH);
            memcpy(ptr + POS_ARRAY_IDX + idx * (OFFSET_WIDTH + SIZE_WIDTH) +
                       OFFSET_WIDTH,
                   &currentSlot.size, SIZE_WIDTH);
        }
        idx++;
    } while (currentSlot.size != PAGE_TAIL);
}

void freePage(Page page) {
    free(page->ptr);
    free(page->header->recordSlots);
    free(page->header);
    free(page);
}

void initialisePageHeader(uint8_t *page) {
    LOG("INITIALISE PAGE HEADER\n");

    const uint16_t numRecords = 0;
    const uint16_t recordStart = _PAGE_SIZE;
    const uint16_t freeSpace = _PAGE_SIZE - NUM_RECORDS_WIDTH * 5;
    const uint16_t tail = PAGE_TAIL;

    memcpy(page, &numRecords, NUM_RECORDS_WIDTH);
    memcpy(page + RECORD_START_IDX, &recordStart, NUM_RECORDS_WIDTH);
    memcpy(page + FREE_SPACE_IDX, &freeSpace, NUM_RECORDS_WIDTH);
    memcpy(page + POS_ARRAY_IDX, &tail, NUM_RECORDS_WIDTH);
    memcpy(page + POS_ARRAY_IDX + OFFSET_WIDTH, &tail, NUM_RECORDS_WIDTH);
}

static QueryResult getFreeSpaces(TableInfo spaceInfo, char *tableName,
                                 size_t recordSize) {
    LOG("GET FREE SPACE\n");

    Condition condition = malloc(sizeof(struct Condition));
    assert(condition != NULL);

    assert(recordSize <= INT_MAX);
    condition->value.twoArg.op2 = createOperand(INT, &recordSize);
    condition->value.twoArg.op1 = SPACE_INFO_FREE_SPACE;
    condition->type = GREATER_THAN;

    Schema *spaceInfoSchema = initSpaceInfoSchema(tableName);
    QueryAttributes spaceInfoIdAttribute =
        createQueryAttributes(1, SPACE_INFO_ID);
    QueryResult res =
        selectFrom(spaceInfo, spaceInfoSchema, condition, spaceInfoIdAttribute);

    free(spaceInfoSchema);
    free(condition->value.twoArg.op2);
    free(condition);
    freeQueryAttributes(spaceInfoIdAttribute);
    return res;
}

static void insertFreeSpace(char *tableName, TableInfo spaceInfo, Page page) {
    LOG("Inserting free space");

    int id = page->pageId;
    int freeSpace = page->header->freeSpace;

    QueryValues values = createQueryValues(3, createOperand(STR, tableName),
                                           createOperand(INT, &id),
                                           createOperand(INT, &freeSpace));

    Schema *spaceInfoSchema = initSpaceInfoSchema(tableName);
    QueryAttributes spaceInfoQueryAttributes = initSpaceInfoQueryAttributes();
    insertInto(spaceInfo, NULL, spaceInfoSchema, initSpaceInfoQueryAttributes(),
               values, FREE_MAP);

    free(spaceInfoQueryAttributes);
    free(spaceInfoSchema);
    freeQueryValues(values);
}

Page nextFreePage(TableInfo tableInfo, TableInfo spaceInfo, size_t recordSize,
                  TableType tableType) {
    LOG("NEXT FREE PAGE\n");
    if (tableType == FREE_MAP || tableType == SCHEMA) {
        LOG("SCHEMA OR MAP FREE PAGE\n");
        for (int i = 0; i < tableInfo->header->numPages; i++) {
            Page page = getPage(tableInfo, tableInfo->header->startPage + i);

            // Compares free space including slot width
            if (page->header->freeSpace >=
                recordSize + OFFSET_WIDTH + SIZE_WIDTH) {
                return page;
            }
        }

        // Adds page if no pages found
        return addPage(tableInfo);
    }

    // Gets all pages with sufficient free space from the space inventory
    QueryResult spaceMapRes =
        getFreeSpaces(spaceInfo, tableInfo->name, recordSize);

    if (spaceMapRes->numRecords == 0) {
        LOG("ADD NEW PAGE TO SPACE MAP\n");
        Page page = addPage(tableInfo);

        insertFreeSpace(tableInfo->name, spaceInfo, page);
        freeRecordArray(spaceMapRes->records);
        free(spaceMapRes);
        return page;
    }

    // Returns first page with sufficient space
    size_t pageId =
        spaceMapRes->records->records[0]->fields[SPACE_ID_IDX + 1].intValue;
    freeRecordArray(spaceMapRes->records);
    free(spaceMapRes);
    LOG("PAGE %ld FOUND\n", pageId);
    return getPage(tableInfo, pageId);
}

void updatePage(TableInfo tableInfo, Page page) {
    LOG("Update page\n");
    writePageHeader(page);
    fseek(tableInfo->table, _PAGE_SIZE * page->pageId, SEEK_SET);
    fwrite(page->ptr, sizeof(uint8_t), _PAGE_SIZE, tableInfo->table);
    fseek(tableInfo->table, 0, SEEK_SET);
}
