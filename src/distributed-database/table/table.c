#include "table.h"

#include <assert.h>
#include <limits.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "buffer/pageBuffer.h"
#include "db-utils.h"
#include "log.h"
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

static void parseQueryValue(Field *field, Operand attributeValue,
                            Attribute attributeName, AttributeType type,
                            unsigned int size) {
    field->attribute = strdup(attributeName);
    field->type = type;

    switch (type) {
        case INT:
            field->intValue = attributeValue->value.intOp;
            field->size = INT_WIDTH;
            break;
        case STR:
            field->stringValue = strdup(attributeValue->value.strOp);
            field->size = size;
            break;
        case VARSTR:
            field->stringValue = strdup(attributeValue->value.strOp);
            field->size = strlen(field->stringValue);
            break;
        case FLOAT:
            field->floatValue = attributeValue->value.floatOp;
            field->size = FLOAT_WIDTH;
            break;
        case BOOL:
            field->boolValue = attributeValue->value.boolOp;
            field->size = BOOL_WIDTH;
            break;
        default:
            LOG_ERROR("Invalid query\n");
    }
}

static void setRecordIdx(Record record, uint32_t globalIdx, int *recordSize) {
    assert(record != NULL);
    record->fields[GLOBAL_ID_RECORD_IDX].attribute = strdup(GLOBAL_ID_NAME);
    record->fields[GLOBAL_ID_RECORD_IDX].type = INT;
    assert(globalIdx <= INT32_MAX);
    record->fields[GLOBAL_ID_RECORD_IDX].intValue = globalIdx;
    record->fields[GLOBAL_ID_RECORD_IDX].size = GLOBAL_ID_WIDTH;

    *recordSize += GLOBAL_ID_WIDTH;
}

Record parseQuery(Schema *schema, QueryAttributes attributes,
                  QueryValues values, uint32_t globalIdx) {
    // Parses query into internal representation

    LOG("PARSE QUERY\n");
    Record record = malloc(sizeof(struct Record));
    assert(record != NULL);

    record->fields = malloc(sizeof(Field) * (schema->numAttributes + 1));
    assert(record->fields != NULL);

    int recordSize = 0;
    setRecordIdx(record, globalIdx, &recordSize);

    record->globalIdx = globalIdx;
    for (int j = 0; j < schema->numAttributes; j++) {
        for (int i = 0; i < attributes->numAttributes; i++) {
            Attribute attributeName = attributes->attributes[i];
            Operand attributeValue = values->values[i];
            AttributeType type = schema->attributeTypes[j];

            // Retrieves schema info with equal attribute name
            if (strcmp(schema->attributes[j], attributeName) == 0) {
                unsigned int attributeSize = schema->attributeSizes[j];

                // Field index offset for global idx field at start
                parseQueryValue(&record->fields[j + 1], attributeValue,
                                attributeName, type, attributeSize);

                recordSize += record->fields[j + 1].size;

                if (record->fields[j + 1].type == VARSTR) {
                    // Adds for offset field
                    recordSize += SLOT_SIZE;
                }
                break;
            }
        }
    }
    // For start of static length fields
    recordSize += RECORD_IDX;

    record->size = recordSize;
    record->numValues = attributes->numAttributes + 1;

    LOG("RECORD PARSE SUCCESSFUL");
    return record;
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
    tableInfo->buffer = initialiseBuffer();

    LOG("Got table\n");
    return tableInfo;
}

void parseField(Field *field, char *attribute, AttributeType type,
                uint16_t size, uint8_t *record) {
    // Parses field from raw binary

    field->attribute = strdup(attribute);
    field->size = size;
    field->type = type;
    switch (type) {
        case INT:
            field->intValue = 0;
            memcpy(&field->intValue, record, field->size);
            break;
        case STR:
            field->stringValue = malloc(sizeof(char) * (field->size + 1));
            assert(field->stringValue != NULL);
            memcpy(field->stringValue, record, field->size);
            field->stringValue[field->size] = '\0';
            break;
        case VARSTR:
            field->stringValue = malloc(sizeof(char) * (field->size + 1));
            assert(field->stringValue != NULL);
            memcpy(field->stringValue, record, field->size);
            field->stringValue[field->size] = '\0';
            break;
        case FLOAT:
            field->floatValue = 0;
            memcpy(&field->floatValue, record, field->size);
            break;
        case BOOL:
            memcpy(&field->boolValue, record, field->size);
            break;
    }
}

Record parseRecord(Page page, size_t offset, Schema *schema) {
    Record record = malloc(sizeof(struct Record));
    assert(record != NULL);

    Field *fields = malloc(sizeof(struct Field) * (schema->numAttributes + 1));
    assert(fields != NULL);

    record->fields = fields;
    record->numValues = schema->numAttributes + 1;
    record->size = 0;

    // Stores offset to start of static fields
    uint16_t staticFieldStart;
    memcpy(&staticFieldStart, page->ptr + offset, RECORD_HEADER_WIDTH);
    uint8_t *recordStart = page->ptr + staticFieldStart;

    // Stores offset to start of variable-length field slots
    uint8_t *slotsStart = page->ptr + offset + RECORD_HEADER_WIDTH;
    RecordSlot slot;

    parseField(fields, GLOBAL_ID_NAME, INT, GLOBAL_ID_WIDTH, recordStart);
    record->globalIdx = fields[0].intValue;
    recordStart += GLOBAL_ID_WIDTH;

    record->size += GLOBAL_ID_WIDTH;
    record->size += RECORD_HEADER_WIDTH;

    for (int i = 0; i < schema->numAttributes; i++) {
        char *attribute = schema->attributes[i];
        AttributeType type = schema->attributeTypes[i];
        if (type == VARSTR) {
            getRecordSlot(slotsStart);
            slotsStart += SLOT_SIZE;
            parseField(fields + i + 1, attribute, type, slot.size,
                       page->ptr + slot.offset);

            // Adds size of slot to variable length field to total record size
            record->size += SLOT_SIZE;
        } else {
            unsigned int size = schema->attributeSizes[i];
            parseField(fields + i + 1, attribute, type, size, recordStart);
            recordStart += size;
        }
        record->size += fields[i + 1].size;
    }

    return record;
}

Record iterateRecords(TableInfo tableInfo, Schema *schema, Buffer buffer,
                      RecordIterator *recordIterator) {
    if (tableInfo->header->numPages == 0) {
        return NULL;
    }

    // Sets iterator fields at start of iteration
    if (recordIterator->page == NULL) {
        recordIterator->pageId = tableInfo->header->startPage;
        recordIterator->page = loadPage(tableInfo, buffer, recordIterator->pageId);
        recordIterator->slotIdx = 0;
    }

    // Stores id of last page stored in memory
    const size_t maxId =
        tableInfo->header->numPages + tableInfo->header->startPage - 1;

    while (recordIterator->pageId <= maxId) {
        RecordSlot *nextSlot =
            &recordIterator->page->header->recordSlots->slots[recordIterator->slotIdx];

        // Iterates over empty slots
        while (nextSlot->size == 0) {
            recordIterator->slotIdx++;
            nextSlot = &recordIterator->page->header->recordSlots->slots[recordIterator->slotIdx];
        }

        // If end of slot array encountered, moves to next page as long as
        // max page not exceeded
        if (nextSlot->size == PAGE_TAIL) {
            recordIterator->pageId++;
            recordIterator->slotIdx = 0;

            if (recordIterator->pageId <= maxId) {
                recordIterator->page =
                    loadPage(tableInfo, recordIterator->pageId);
            }
        } else {
            recordIterator->slotIdx++;
            recordIterator->lastSlot = nextSlot;
            return parseRecord(recordIterator->page, nextSlot->offset, schema);
        }
    }
    return NULL;
}

RecordSlot getRecordSlot(uint8_t *idx) {
    RecordSlot slot;
    memcpy(&slot.offset, idx, OFFSET_WIDTH);
    memcpy(&slot.size, idx + OFFSET_WIDTH, SIZE_WIDTH);
    slot.pos = idx;
    return slot;
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
    while (slots[cnt].size != PAGE_TAIL) {
        cnt++;
    }
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
    int numSlots = countSlots(page->header->recordSlots->slots);
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

void writeField(Page page, uint16_t offset, Field field) {
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

    memcpy(page->ptr + offset, value, field.size);
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
                          Frame *frame) {
    LOG("Update space inventory\n");

    Condition cond = malloc(sizeof(struct Condition));
    assert(cond != NULL);

    cond->type = EQUALS;
    cond->value.twoArg.op1 = SPACE_INFO_ID;

    Page page = getPageFromFrame(frame);

    size_t id = page->pageId;
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

static int countNumVarFields(Record record) {
    int numVar = 0;
    for (int i = 0; i < record->numValues; i++) {
        Field field = record->fields[i];
        if (field.type == VARSTR) {
            numVar++;
        }
    }
    return numVar;
}

uint16_t writeRecord(Page page, Record record, uint32_t globalIdx,
                     uint16_t recordEnd) {
    LOG("Write record %d\n", globalIdx);
    // Offset to start of record
    uint16_t recordStart = recordEnd - record->size;
    // Offset to end of record slots/start of static fields

    // Counts number of variable length fields to determine number of variable
    // field slots needed
    int numVar = countNumVarFields(record);

    uint16_t staticFieldStart =
        recordStart + RECORD_HEADER_WIDTH + numVar * (SLOT_SIZE);
    uint16_t slotEnd = staticFieldStart;

    // Variable length fields must be at end of record
    assert(record->numValues <= INT_MAX);
    for (int i = record->numValues - 1; i >= 0; i--) {
        Field field = record->fields[i];
        recordEnd -= field.size;
        writeField(page->ptr + recordEnd, field);

        LOG("Wrote %s\n", field.attribute);
        if (field.type == VARSTR) {
            // Writes (pos, width) slot for each variable length field, updating
            // start of static fields
            slotEnd -= OFFSET_WIDTH + SIZE_WIDTH;
            copyToPage(page, slotEnd, &recordEnd, OFFSET_WIDTH);
            copyToPage(page, slotEnd + OFFSET_WIDTH, &field.size, SIZE_WIDTH);
        }
    }
    // Writes offset to start of static length fields
    copyToPage(page, recordStart, &staticFieldStart, RECORD_HEADER_WIDTH);

    return recordStart;
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
    freeBuffer(tableInfo->buffer);
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
