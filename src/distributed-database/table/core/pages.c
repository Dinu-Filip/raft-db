#include "pages.h"

#include <assert.h>
#include <limits.h>
#include <stdlib.h>
#include <string.h>

#include "../operations/insert.h"
#include "../schema.h"
#include "log.h"
#include "record.h"
#include "recordArray.h"
#include "table/operations/operation.h"
#include "table/operations/sqlToOperation.h"

#define INITIAL_NUM_SLOTS 10

static void initialisePageHeaderSlots(PageHeader header) {
    unsigned capacity = header->slots.size > 0 ? header->slots.size : 1;

    header->slots.slots = malloc(sizeof(RecordSlot) * capacity);
    assert(header->slots.slots != NULL);

    header->slots.capacity = capacity;
}

static void readPageSlots(PageHeader header, uint8_t *ptr) {
    initialisePageHeaderSlots(header);
    RecordSlot *slots = header->slots.slots;

    uint8_t *start = ptr + POS_ARRAY_IDX;
    unsigned numRecords = 0;

    // Reads page slots into header
    for (int i = 0; i < header->slots.size; i++) {
        getRecordSlot(&slots[i], start);
        slots[i].modified = false;
        start += SLOT_SIZE;

        // Counts number of non-empty slots to get number of records in page
        if (slots[i].size != 0) {
            numRecords++;
        }
    }

    header->numRecords = numRecords;
}

void getRecordSlot(RecordSlot *slot, uint8_t *idx) {
    memcpy(&slot->offset, idx, OFFSET_WIDTH);
    memcpy(&slot->size, idx + OFFSET_WIDTH, SIZE_WIDTH);
    slot->pos = idx;
}

static PageHeader getPageHeader(uint8_t *ptr) {
    PageHeader pageHeader = calloc(1, sizeof(struct PageHeader));
    assert(pageHeader != NULL);

    memcpy(&pageHeader->slots.size, ptr + NUM_SLOTS_IDX, NUM_SLOTS_WIDTH);
    memcpy(&pageHeader->recordStart, ptr + RECORD_START_IDX, NUM_SLOTS_WIDTH);
    memcpy(&pageHeader->freeSpace, ptr + FREE_SPACE_IDX, NUM_SLOTS_WIDTH);

    pageHeader->modified = false;

    // Reads page slots pointing pointer and size of each record
    readPageSlots(pageHeader, ptr);

    return pageHeader;
}

uint8_t *getRawPage(FILE *table, size_t pageSize, size_t pageId) {
    uint8_t *ptr = malloc(sizeof(uint8_t) * pageSize);
    assert(ptr != NULL);

    size_t pageStart = pageId * pageSize;
    assert(pageStart <= LONG_MAX);

    // Reads raw page from database file
    fseek(table, pageStart, SEEK_SET);
    fread(ptr, sizeof(uint8_t), pageSize, table);
    fseek(table, 0, SEEK_SET);

    return ptr;
}

Page getPage(TableInfo table, size_t pageId) {
    const size_t pageSize = table->header->pageSize;

    Page page = malloc(sizeof(struct Page));
    assert(page != NULL);

    uint8_t *ptr = getRawPage(table->table, pageSize, pageId);

    page->ptr = ptr;
    page->header = getPageHeader(ptr);
    page->pageId = pageId;

    fseek(table->table, 0, SEEK_SET);
    return page;
}

Page addPage(TableInfo table) {
    size_t pageSize = table->header->pageSize;

    Page page = malloc(sizeof(struct Page));
    assert(page != NULL);

    uint8_t *ptr = malloc(sizeof(uint8_t) * pageSize);
    assert(ptr != NULL);

    table->header->numPages++;

    page->ptr = ptr;
    page->header = initialisePageHeader();
    page->pageId = table->header->numPages;
    // The page is written back to the database file lazily
    table->header->modified = true;

    return page;
}

static void writePageHeader(Page page) {
    LOG("Update page header");
    PageHeader header = page->header;

    if (!header->modified) {
        return;
    }

    uint8_t *ptr = page->ptr;

    memcpy(ptr + NUM_SLOTS_IDX, &header->slots.size, NUM_SLOTS_WIDTH);
    memcpy(ptr + RECORD_START_IDX, &header->recordStart, NUM_SLOTS_WIDTH);
    memcpy(ptr + FREE_SPACE_IDX, &header->freeSpace, NUM_SLOTS_WIDTH);

    for (int i = 0; i < header->slots.size; i++) {
        RecordSlot currentSlot = header->slots.slots[i];

        // Writes only modified slots to the page
        if (!currentSlot.modified) {
            continue;
        }

        memcpy(ptr + POS_ARRAY_IDX + i * (OFFSET_WIDTH + SIZE_WIDTH),
               &currentSlot.offset, OFFSET_WIDTH);
        memcpy(ptr + POS_ARRAY_IDX + i * (OFFSET_WIDTH + SIZE_WIDTH) +
                   OFFSET_WIDTH,
               &currentSlot.size, SIZE_WIDTH);
    }
}

void freePage(Page page) {
    free(page->ptr);
    free(page->header->slots.slots);
    free(page->header);
    free(page);
}

PageHeader initialisePageHeader() {
    PageHeader header = malloc(sizeof(struct PageHeader));
    assert(header != NULL);

    header->numRecords = 0;
    header->recordStart = _PAGE_SIZE;
    header->freeSpace = _PAGE_SIZE - NUM_SLOTS_WIDTH * 3;

    header->slots.size = 0;
    initialisePageHeaderSlots(header);

    header->modified = true;

    return header;
}

static void resizeRecordSlots(RecordSlotArray *array) {
    if (array->size < array->capacity) {
        return;
    }

    array->capacity *= 2;
    array->slots = realloc(array->slots, sizeof(RecordSlot) * array->capacity);
    assert(array->slots != NULL);
}

void updatePageHeaderInsert(Record record, Page page, uint16_t recordStart) {
    // Updates free space log in page header
    page->header->freeSpace -= record->size;

    page->header->modified = true;
    page->header->numRecords++;
    // Record always inserted at beginning of free space
    page->header->recordStart = recordStart;

    // First tries to fill empty slot
    for (int i = 0; i < page->header->slots.size; i++) {
        RecordSlot *slot = &page->header->slots.slots[i];
        if (slot->size == 0) {
            slot->size = record->size;
            slot->offset = recordStart;
            slot->modified = true;
            return;
        }
    }

    // All slots must be full, so adds slot to end
    resizeRecordSlots(&page->header->slots);
    RecordSlot *newSlot =
        &page->header->slots.slots[page->header->slots.size++];
    newSlot->size = record->size;
    newSlot->offset = recordStart;
    newSlot->modified = true;
    page->header->freeSpace -= SLOT_SIZE;
}

static QueryResult getFreeSpaces(TableInfo spaceInfo, size_t recordSize) {
    char template[] = "select * from %s where %s >= %d;";
    char sql[100];
    snprintf(sql, sizeof(sql), template, spaceInfo->name,
             SPACE_TABLE_FREE_SPACE, recordSize);

    QueryResult res = executeQualifiedOperation(sqlToOperation(sql), FREE_MAP);

    return res;
}

static void insertFreeSpace(TableInfo spaceInfo, Page page) {
    LOG("Inserting free space");

    int id = page->pageId;
    int freeSpace = page->header->freeSpace;

    char template[] = "insert into %s values (%d, %d);";
    char sql[100];
    snprintf(sql, sizeof(sql), template, spaceInfo->name, id, freeSpace);

    executeQualifiedOperation(sqlToOperation(sql), FREE_MAP);
}

Page nextFreePage(TableInfo tableInfo, TableInfo spaceInfo, size_t recordSize,
                  TableType tableType) {
    if (tableType == FREE_MAP || tableType == SCHEMA) {
        for (int i = 0; i < tableInfo->header->numPages; i++) {
            Page page = getPage(tableInfo, 1 + i);

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
    QueryResult spaceMapRes = getFreeSpaces(spaceInfo, recordSize);

    if (spaceMapRes->records->size == 0) {
        Page page = addPage(tableInfo);

        insertFreeSpace(spaceInfo, page);
        freeRecordArray(spaceMapRes->records);
        free(spaceMapRes);
        return page;
    }

    // Returns first page with sufficient space
    size_t pageId =
        spaceMapRes->records->records[0]->fields[SPACE_ID_IDX].intValue;

    freeRecordArray(spaceMapRes->records);
    free(spaceMapRes);

    return getPage(tableInfo, pageId);
}

void updatePage(TableInfo tableInfo, Page page) {
    // Updates page header if modified
    writePageHeader(page);

    fseek(tableInfo->table, _PAGE_SIZE * page->pageId, SEEK_SET);
    fwrite(page->ptr, sizeof(uint8_t), _PAGE_SIZE, tableInfo->table);
    fseek(tableInfo->table, 0, SEEK_SET);
}

void defragmentRecords(Page page) {
    unsigned numSlots = page->header->slots.size;

    RecordSlot **slots = malloc(sizeof(RecordSlot *) * numSlots);
    assert(slots != NULL);

    for (int i = 0; i < numSlots; i++) {
        slots[i] = &page->header->slots.slots[i];
    }

    // Sorts pointers to slots in decreasing order of offset.
    // This ensures there is no overlap when shifting records
    qsort(slots, numSlots, sizeof(RecordSlot *), &compareSlots);

    size_t recordStart = _PAGE_SIZE;

    // Keeps track of old offset to first record
    unsigned oldStart = page->header->recordStart;

    unsigned numRecords = page->header->numRecords;

    // Ensures that any empty slots are moved to the end
    if (numRecords < page->header->slots.size) {
        assert(slots[page->header->numRecords]->offset == 0);
    }

    for (int i = 0; i < numRecords; i++) {
        RecordSlot *slot = slots[i];
        slot->modified = true;

        // Calculates expected offset of record from end of page
        recordStart -= slot->size;

        // Skips if record already in correct position
        if (slot->offset == recordStart) {
            continue;
        }

        // Shifts record if size of record is less than offset to next record
        memmove(page->ptr + recordStart, page->ptr + slot->offset,
                sizeof(uint8_t) * slot->size);

        slot->modified = true;
        slot->offset = recordStart;
    }

    // Adds recovered space
    page->header->freeSpace += recordStart - oldStart;

    page->header->recordStart = recordStart;
    page->header->modified = true;

    free(slots);
}