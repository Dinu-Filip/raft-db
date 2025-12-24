#include "table.h"

#include <assert.h>
#include <limits.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "core/record.h"
#include "db-utils.h"
#include "log.h"
#include "pages.h"
#include "schema.h"
#include "table/operation.h"
#include "update.h"

// Global database directory
char DB_DIRECTORY[MAX_FILE_NAME_LEN] = {'\0'};

int getStaticTypeWidth(AttributeType type) {
    switch (type) {
        case INT:
            return INT_WIDTH;
        case FLOAT:
            return FLOAT_WIDTH;
        case BOOL:
            return BOOL_WIDTH;
        default:
            LOG_ERROR("Invalid type\n");
    }
}

void freeTable(TableInfo tableInfo) {
    fclose(tableInfo->table);
    free(tableInfo->header);
    free(tableInfo);
}

RecordArray createRecordArray() {
    RecordArray recordArray = malloc(sizeof(struct RecordArray));
    assert(recordArray != NULL);

    Record *records = malloc(sizeof(Record) * INITIAL_RECORD_CAPACITY);
    assert(records != NULL);

    recordArray->records = records;
    recordArray->capacity = INITIAL_RECORD_CAPACITY;
    recordArray->size = 0;

    return recordArray;
}

void freeRecordArray(RecordArray records) {
    for (int i = 0; i < records->size; i++) {
        Record record = records->records[i];
        freeRecord(record);
    }
    free(records->records);
    free(records);
}

void resizeRecordArray(RecordArray records) {
    if (records->capacity == records->size) {
        records->capacity *= 2;
        records->records =
            realloc(records->records, sizeof(Record) * records->capacity);
        assert(records->records != NULL);
    }
}

void addRecord(RecordArray records, Record record) {
    records->records[records->size++] = record;
    resizeRecordArray(records);
}

TableInfo openTable(char *tableName) {
    char tableFile[MAX_FILE_NAME_LEN + MAX_TABLE_NAME_LEN];
    snprintf(tableFile, MAX_FILE_NAME_LEN + MAX_TABLE_NAME_LEN, "%s/%s.%s",
             DB_DIRECTORY, tableName, DB_EXTENSION);
    FILE *table = fopen(tableFile, "rb+");
    LOG("Open %s\n", tableFile);

    TableInfo tableInfo = malloc(sizeof(struct TableInfo));
    assert(tableInfo != NULL);

    tableInfo->table = table;
    tableInfo->name = strdup(tableName);
    tableInfo->header = getTableHeader(table);

    LOG("Got table\n");
    return tableInfo;
}

void getRecordSlot(RecordSlot *slot, uint8_t *idx) {
    memcpy(&slot->offset, idx, OFFSET_WIDTH);
    memcpy(&slot->size, idx + OFFSET_WIDTH, SIZE_WIDTH);
    slot->pos = idx;
}

int compareSlots(const void *slot1, const void *slot2) {
    // Compares slots in decreasing order of offset

    RecordSlot *s1 = *(RecordSlot **)slot1;
    RecordSlot *s2 = *(RecordSlot **)slot2;
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
    uint16_t pageSize;

    memcpy(&pageSize, headerPage + PAGE_SIZE_IDX, PAGE_SIZE_WIDTH);

    uint32_t numPages;
    memcpy(&numPages, headerPage + NUM_PAGES_IDX, PAGE_ID_WIDTH);

    int startPage;
    memcpy(&startPage, headerPage + START_PAGE_IDX, PAGE_ID_WIDTH);

    uint32_t globalIdx;
    memcpy(&globalIdx, headerPage + GLOBAL_ID_IDX, GLOBAL_ID_WIDTH);

    header->pageSize = pageSize;
    header->numPages = numPages;
    header->startPage = startPage;

    header->globalIdx = globalIdx;

    LOG("Get page size: %ld", header->pageSize);
    LOG("Get num pages: %ld", header->numPages);
    LOG("Get start page: %d", header->startPage);
    LOG("Get global index: %d", header->globalIdx);

    header->modified = false;

    free(headerPage);
    return header;
}

void freeTableHeader(TableHeader tableHeader) { free(tableHeader); }

static int countSlots(RecordSlot *slots) {
    int cnt = 0;
    while (slots[cnt++].size != PAGE_TAIL);
    return cnt;
}

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
    int numSlots = countSlots(page->header->recordSlots);

    // Sorts slots in decreasing order of offset
    // This allows records to be shifted as far right as possible without
    // overwriting existing data
    RecordSlot **slots = malloc(sizeof(RecordSlot *) * numSlots);
    assert(slots != NULL);

    for (int i = 0; i < numSlots; i++) {
        slots[i] = &page->header->recordSlots[i];
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

void initialiseRecordIterator(RecordIterator *iterator) {
    iterator->page = NULL;
    iterator->pageId = 0;
    iterator->slotIdx = 0;
}

void writeField(uint8_t *fieldStart, Field field) {
    void *value;

    switch (field.type) {
        case INT:
            value = &field.intValue;
            break;
        case STR:
        case VARSTR:
            value = field.stringValue;
            break;
        case FLOAT:
            value = &field.floatValue;
            break;
        case BOOL:
            value = &field.boolValue;
            break;
        default:
            LOG_ERROR("Invalid field\n");
    }

    memcpy(fieldStart, value, field.size);
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
        fwrite(&header->startPage, sizeof(uint8_t), PAGE_ID_WIDTH,
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

void outputField(Field field, unsigned int rightPadding) {
    switch (field.type) {
        case INT:
            fprintf(stderr, "%-*d", rightPadding, field.intValue);
            break;
        case FLOAT:
            fprintf(stderr, "%-*f", rightPadding, field.floatValue);
            break;
        case BOOL:
            fprintf(stderr, "%-*d", rightPadding, field.boolValue);
            break;
        case STR:
        case VARSTR:
            fprintf(stderr, "%-*s", rightPadding, field.stringValue);
            break;
        default:
            break;
    }
}

void outputRecord(Record record) {
    for (int i = 0; i < record->numValues; i++) {
        Field field = record->fields[i];
        fprintf(stderr, "%s has type %d, size %d and value ", field.attribute,
                field.type, field.size);
        outputField(field, 0);
        fprintf(stderr, "\n");
    }
}

void closeTable(TableInfo tableInfo) {
    fclose(tableInfo->table);
    free(tableInfo->header);
    free(tableInfo->name);
    free(tableInfo);
}

void freeRecordIterator(RecordIterator iterator) {}

void freeRecord(Record record) {
    for (int j = 0; j < record->numValues; j++) {
        if (record->fields[j].type == VARSTR || record->fields[j].type == STR) {
            free(record->fields[j].stringValue);
        }
        free(record->fields[j].attribute);
    }
    free(record->fields);
    free(record);
}

void freeField(Field field) {
    free(field.attribute);
    if (field.type == STR || field.type == VARSTR) {
        free(field.stringValue);
    }
}
