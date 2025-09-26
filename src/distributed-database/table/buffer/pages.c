#include <assert.h>
#include <limits.h>
#include <stdlib.h>
#include <string.h>

#include "pages.h"
#include "../insert.h"
#include "../schema.h"
#include "../select.h"
#include "log.h"
#include "table/operation.h"

#define INITIAL_NUM_SLOTS 10

struct SlotArray {
    RecordSlot *slots;
    size_t size;
    size_t capacity;
};

SlotArray initSlotArray() {
    SlotArray slots = malloc(sizeof(struct SlotArray));
    assert(slots != NULL);

    slots->slots = malloc(sizeof(RecordSlot) * INITIAL_NUM_SLOTS);
    slots->size = 0;
    slots->capacity = INITIAL_NUM_SLOTS;

    return slots;
}

void addSlot(SlotArray slotArray, RecordSlot slot) {
    if (slotArray->size == slotArray->capacity) {
        slotArray->capacity *= 2;

        slotArray->slots = realloc(slotArray->slots, slotArray->capacity * sizeof(RecordSlot));
        assert(slotArray->slots != NULL);
    }

    slotArray->slots[slotArray->size++] = slot;
}

static SlotArray readSlots(void *start) {
    SlotArray slots = initSlotArray();
    RecordSlot slot;
    do {
        slot = getRecordSlot(start);
        start += SLOT_SIZE;
        addSlot(slots, slot);
    } while (slot.size != PAGE_TAIL);

    return slots;
}

static void freeSlots(SlotArray slots) {
    free(slots->slots);
    free(slots);
}

static PageHeader loadPageHeader(Page page) {
    PageHeader pageHeader = malloc(sizeof(struct PageHeader));
    assert(pageHeader != NULL);

    copyFromPage(page, NUM_RECORDS_IDX, &pageHeader->numRecords, NUM_RECORDS_WIDTH);
    copyFromPage(page, RECORD_START_IDX, &pageHeader->numRecords, NUM_RECORDS_WIDTH);
    copyFromPage(page, FREE_SPACE_IDX, &pageHeader->numRecords, NUM_RECORDS_WIDTH);

    pageHeader->modified = false;

    pageHeader->recordSlots = readSlots(page->ptr + POS_ARRAY_IDX);

    return pageHeader;
}

static void copyFromPage(Page page, uint16_t offset, void *dest, int n) {
    memcpy(dest, page->ptr + offset, n);
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
    page->header = loadPageHeader(ptr);
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
    page->header = loadPageHeader(ptr);
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

static void updateSlots(void *start, SlotArray slots) {
    int idx = 0;
    RecordSlot currentSlot;
    do {
        currentSlot = slots->slots[idx];
        if (currentSlot.modified) {
            memcpy(start + POS_ARRAY_IDX + idx * SLOT_SIZE, &currentSlot.offset, OFFSET_WIDTH);
            memcpy(start + POS_ARRAY_IDX + idx * SLOT_SIZE + OFFSET_WIDTH, &currentSlot.size, SIZE_WIDTH);
        }
        idx++;
    } while (currentSlot.size != PAGE_TAIL);
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

    updateSlots(ptr, page->header->recordSlots);
}

void freePage(Page page) {
    free(page->ptr);
    freeSlots(page->header->recordSlots);
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

static void updatePage(TableInfo tableInfo, Page page) {
    LOG("Update page\n");
    writePageHeader(page);
    fseek(tableInfo->table, _PAGE_SIZE * page->pageId, SEEK_SET);
    fwrite(page->ptr, sizeof(uint8_t), _PAGE_SIZE, tableInfo->table);
    fseek(tableInfo->table, 0, SEEK_SET);
}
