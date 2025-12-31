#include "record.h"

#include <assert.h>
#include <string.h>

#include "log.h"
#include "table/core/pages.h"

static Record initialiseRecord(unsigned numAttributes) {
    Record record = malloc(sizeof(struct Record));
    assert(record != NULL);

    // Allocates array of fields of record
    record->fields = malloc(sizeof(Field) * numAttributes);
    assert(record->fields != NULL);

    record->numValues = numAttributes;
    return record;
}

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

static void parseQueryValueToField(Field *field, Operand attributeValue,
                                   AttributeName attributeName,
                                   AttributeType type, unsigned size) {
    field->attribute = strdup(attributeName);
    field->type = type;
    field->size = size;

    switch (type) {
        case INT:
            field->intValue = attributeValue->value.intOp;
            break;
        case STR:
            field->stringValue = strdup(attributeValue->value.strOp);
            break;
        case VARSTR:
            field->stringValue = strdup(attributeValue->value.strOp);
            field->size = strlen(field->stringValue);
            break;
        case FLOAT:
            field->floatValue = attributeValue->value.floatOp;
            break;
        case BOOL:
            field->boolValue = attributeValue->value.boolOp;
            break;
        default:
            LOG_ERROR("Invalid query\n");
    }
}

Record parseQuery(Schema *schema, QueryAttributes attributes,
                  QueryValues values, uint32_t globalIdx) {
    // Parses query into internal representation
    unsigned numAttributes = attributes->numAttributes;
    Record record = initialiseRecord(numAttributes);

    // Computes the size of the record data in bytes
    unsigned recordSize = 0;

    // Sets global index and adds to record size
    record->globalIdx = globalIdx;
    recordSize += GLOBAL_ID_WIDTH;

    // Iterates over query, matching schema to attributes
    for (int i = 0; i < numAttributes; i++) {
        AttributeName attributeName = attributes->attributes[i];
        for (int j = 0; j < schema->numAttrs; j++) {
            AttrInfo info = schema->attrInfos[j];

            // Skips if attribute names do not match
            if (strcmp(info.name, attributeName) != 0) {
                continue;
            }

            Operand attributeValue = values->values[i];

            // Sets field of record to attribute values and metadata
            // The order of the fields always matches the schema order
            parseQueryValueToField(&record->fields[j], attributeValue,
                                   attributeName, info.type, info.size);

            recordSize += record->fields[j].size;

            if (record->fields[j].type == VARSTR) {
                // Adds for variable offset field
                recordSize += SLOT_SIZE;
            }

            break;
        }
    }

    // For start of static length fields
    recordSize += RECORD_HEADER_WIDTH;

    record->size = recordSize;

    // Number of attribute values excluding global index
    record->numValues = attributes->numAttributes;

    return record;
}

static void parseField(Field *field, char *attribute, AttributeType type,
                       uint16_t size, uint8_t *fieldptr) {
    // Parses field from raw binary

    field->attribute = strdup(attribute);
    field->size = size;
    field->type = type;

    switch (type) {
        case INT:
            memcpy(&field->intValue, fieldptr, field->size);
            break;
        case STR:
            field->stringValue = malloc(sizeof(char) * (field->size + 1));
            assert(field->stringValue != NULL);
            memcpy(field->stringValue, fieldptr, field->size);
            field->stringValue[field->size] = '\0';
            break;
        case VARSTR:
            field->stringValue = malloc(sizeof(char) * (field->size + 1));
            assert(field->stringValue != NULL);
            memcpy(field->stringValue, fieldptr, field->size);
            field->stringValue[field->size] = '\0';
            break;
        case FLOAT:
            memcpy(&field->floatValue, fieldptr, field->size);
            break;
        case BOOL:
            memcpy(&field->boolValue, fieldptr, field->size);
            break;
        case ATTR:
            exit(-1);
    }
}

uint16_t getFieldOffset(uint8_t *recordPtr, unsigned slotIdx) {
    uint16_t offset;
    memcpy(&offset, recordPtr + RECORD_HEADER_WIDTH + OFFSET_WIDTH * slotIdx,
           OFFSET_WIDTH);
    return offset;
}

uint16_t getFieldSize(uint8_t *recordPtr, unsigned slotIdx) {
    uint16_t offset1 = getFieldOffset(recordPtr, slotIdx);
    uint16_t offset2 = getFieldOffset(recordPtr, slotIdx + 1);
    return offset2 - offset1;
}

Record parseRecord(uint8_t *ptr, Schema *schema) {
    unsigned numAttrs = schema->numAttrs;
    Record record = initialiseRecord(numAttrs);

    Field *fields = record->fields;
    record->size = 0;

    // Read offset to start of static fields
    uint16_t numVars;
    memcpy(&numVars, ptr, RECORD_HEADER_WIDTH);
    uint8_t *recordStart = ptr + RECORD_HEADER_WIDTH + numVars * OFFSET_WIDTH;

    // Reads the global index
    memcpy(&record->globalIdx, recordStart, GLOBAL_ID_WIDTH);

    recordStart += GLOBAL_ID_WIDTH;

    record->size += GLOBAL_ID_WIDTH;
    record->size += RECORD_HEADER_WIDTH;

    // Adds for sentinel offset
    record->size += OFFSET_WIDTH;

    // Reads each attribute from record using schema
    for (int i = 0; i < numAttrs; i++) {
        AttrInfo info = schema->attrInfos[i];
        unsigned size;
        uint8_t *fieldptr;

        if (info.type == VARSTR) {
            // Retrieves slot for variable-length field
            size = getFieldSize(ptr, info.loc);
            fieldptr = ptr + getFieldOffset(ptr, info.loc);

            // Adds size of slot to variable length field to total record size
            record->size += OFFSET_WIDTH;
        } else {
            fieldptr = recordStart + info.loc;
            size = info.size;
        }

        parseField(fields + i, info.name, info.type, size, fieldptr);

        // Adds size of parsed field
        record->size += fields[i].size;
    }

    return record;
}

static unsigned countNumVarFields(Record record) {
    int numVar = 0;
    for (int i = 0; i < record->numValues; i++) {
        Field field = record->fields[i];
        if (field.type == VARSTR) {
            numVar++;
        }
    }
    return numVar;
}

void writeRecord(uint8_t *ptr, Record record) {
    // Counts number of variable length fields to determine number of variable
    // field slots needed
    // Adds 1 for sentinel slot at end of record
    uint16_t numVar = 0;
    uint16_t varOffset = 0;
    for (int i = 0; i < record->numValues; i++) {
        Field field = record->fields[i];
        if (field.type == VARSTR) {
            numVar++;
        } else {
            varOffset += field.size;
        }
    }
    // Adds for sentinel offset
    numVar++;
    varOffset += GLOBAL_ID_WIDTH;

    // Writes offset to start of static fields
    memcpy(ptr, &numVar, RECORD_HEADER_WIDTH);

    unsigned slotOffset = RECORD_HEADER_WIDTH;
    unsigned staticFieldOffset = slotOffset + numVar * OFFSET_WIDTH;
    unsigned varFieldOffset = staticFieldOffset + varOffset;

    memcpy(ptr + staticFieldOffset, &record->globalIdx, GLOBAL_ID_WIDTH);
    staticFieldOffset += GLOBAL_ID_WIDTH;

    // Writes each field, setting (offset, size) slot for each variable length
    // field
    for (int i = 0; i < record->numValues; i++) {
        Field field = record->fields[i];
        if (field.type == VARSTR) {
            writeField(ptr + varFieldOffset, field);

            memcpy(ptr + slotOffset, &varFieldOffset, OFFSET_WIDTH);
            slotOffset += OFFSET_WIDTH;

            varFieldOffset += field.size;
        } else {
            writeField(ptr + staticFieldOffset, field);
            staticFieldOffset += field.size;
        }
    }
    // Adds sentinel offset
    memcpy(ptr + slotOffset, &record->size, OFFSET_WIDTH);
}

void initialiseRecordIterator(RecordIterator iterator) {
    iterator->page = NULL;
    iterator->pageId = 1;
    iterator->slotIdx = 0;
}

bool iterateRecords(TableInfo tableInfo,
                      RecordIterator recordIterator, bool autoClearPage) {
    // Checks if database is empty
    if (tableInfo->header->numPages == 0) {
        return false;
    }

    // Stores id of last page stored in memory
    const size_t maxId = tableInfo->header->numPages;

    while (recordIterator->pageId <= maxId) {
        // Sets iterator fields at start of iteration
        if (recordIterator->page == NULL) {
            recordIterator->page = getPage(tableInfo, recordIterator->pageId);
            recordIterator->slotIdx = 0;
        }

        // If end of slot array encountered, moves to next page
        if (recordIterator->slotIdx ==
            recordIterator->page->header->slots.size) {
            recordIterator->pageId++;

            // Frees previous page if not used elsewhere
            if (autoClearPage) {
                freePage(recordIterator->page);
            }

            recordIterator->page = NULL;
            continue;
        }

        // Reads next record slot
        RecordSlot *nextSlot = &recordIterator->page->header->slots
                                    .slots[recordIterator->slotIdx++];

        // Skips over empty slot
        if (nextSlot->size == 0) {
            continue;
        }

        recordIterator->lastSlot = nextSlot;
        return true;
    }

    return false;
}

void freeRecordIterator(RecordIterator iterator) {}

void outputRecord(Record record) {
    for (int i = 0; i < record->numValues; i++) {
        Field field = record->fields[i];
        fprintf(stderr, "%s has type %d, size %d and value ", field.attribute,
                field.type, field.size);
        outputField(field, 0);
        fprintf(stderr, "\n");
    }
}

void removeRecord(Page page, RecordSlot *slot, size_t recordSize) {
    slot->size = 0;
    slot->modified = true;
    page->header->modified = true;
    page->header->freeSpace += recordSize + SLOT_SIZE;
    page->header->numRecords--;
}
