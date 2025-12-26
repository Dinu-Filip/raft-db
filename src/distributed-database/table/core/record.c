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

    LOG("PARSE QUERY\n");
    Record record = initialiseRecord(schema->numAttributes);

    // Computes the size of the record data in bytes
    unsigned recordSize = 0;

    // Sets global index and adds to record size
    record->globalIdx = globalIdx;
    recordSize += GLOBAL_ID_WIDTH;

    // Iterates over query, matching schema to attributes
    for (int j = 0; j < schema->numAttributes; j++) {
        for (int i = 0; i < attributes->numAttributes; i++) {
            AttributeName attributeName = attributes->attributes[i];

            // Skips if attribute names do not match
            if (strcmp(schema->attributes[j], attributeName) != 0) {
                continue;
            }

            Operand attributeValue = values->values[i];
            AttributeType type = schema->attributeTypes[j];
            unsigned attributeSize = schema->attributeSizes[j];

            // Sets field of record to attribute values and metadata
            parseQueryValueToField(&record->fields[j], attributeValue,
                                   attributeName, type, attributeSize);

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

    // Number of attribute values including global index
    record->numValues = attributes->numAttributes + 1;

    LOG("RECORD PARSE SUCCESSFUL");
    return record;
}

static void parseField(Field *field, char *attribute, AttributeType type,
                uint16_t size, uint8_t *record) {
    // Parses field from raw binary

    field->attribute = strdup(attribute);
    field->size = size;
    field->type = type;

    switch (type) {
        case INT:
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
            memcpy(&field->floatValue, record, field->size);
            break;
        case BOOL:
            memcpy(&field->boolValue, record, field->size);
            break;
    }
}

Record parseRecord(uint8_t *ptr, Schema *schema) {
    Record record = initialiseRecord(schema->numAttributes);

    Field *fields = record->fields;
    record->size = 0;

    // Read offset to start of static fields
    uint16_t staticFieldStart;
    memcpy(&staticFieldStart, ptr, RECORD_HEADER_WIDTH);
    uint8_t *recordStart = ptr + staticFieldStart;

    // Reads the global index
    memcpy(&record->globalIdx, recordStart, GLOBAL_ID_WIDTH);

    recordStart += GLOBAL_ID_WIDTH;

    record->size += GLOBAL_ID_WIDTH;
    record->size += RECORD_HEADER_WIDTH;

    // Stores pointer to start of variable-length field slots
    uint8_t *slotsStart = ptr + RECORD_HEADER_WIDTH;
    RecordSlot slot;

    // Reads each attribute from record using schema. As a precondition, the
    // variable length schema attributes should be at the end.
    for (int i = 0; i < schema->numAttributes; i++) {
        char *attribute = schema->attributes[i];
        AttributeType type = schema->attributeTypes[i];
        if (type == VARSTR) {
            // Retrieves slot for variable-length field
            getRecordSlot(&slot, slotsStart);

            // Moves to next slot
            slotsStart += SLOT_SIZE;

            // Parse variable length field using slot offset and size
            parseField(fields + i, attribute, type, slot.size,
                       ptr + slot.offset);

            // Adds size of slot to variable length field to total record size
            record->size += SLOT_SIZE;
        } else {
            unsigned size = schema->attributeSizes[i];

            // Parses field from record
            parseField(fields + i, attribute, type, size, recordStart);

            recordStart += size;
        }

        // Adds size of parsed field
        record->size += fields[i].size;
    }

    return record;
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

void writeRecord(uint8_t *ptr, Record record) {
    LOG("Write record %d\n", record->globalIdx);

    // Counts number of variable length fields to determine number of variable
    // field slots needed
    int numVar = countNumVarFields(record);

    // Writes offset to start of static fields
    unsigned staticFieldStart = RECORD_HEADER_WIDTH + numVar * SLOT_SIZE;
    memcpy(ptr, &staticFieldStart, RECORD_HEADER_WIDTH);

    unsigned slotOffset = RECORD_HEADER_WIDTH;
    unsigned fieldOffset = staticFieldStart;

    memcpy(ptr + fieldOffset, &record->globalIdx, GLOBAL_ID_WIDTH);
    fieldOffset += GLOBAL_ID_WIDTH;

    // Writes each field, setting (offset, size) slot for each variable length
    // field
    for (int i = 0; i < record->numValues; i++) {
        Field field = record->fields[i];
        writeField(ptr + fieldOffset, field);
        if (field.type == VARSTR) {
            // Writes (pos, width) slot for each variable length field, updating
            // start of static fields
            memcpy(ptr + slotOffset, &fieldOffset, OFFSET_WIDTH);
            memcpy(ptr + slotOffset + OFFSET_WIDTH, &field.size, SIZE_WIDTH);
            slotOffset += SLOT_SIZE;
        }
        fieldOffset += field.size;
    }
}

void initialiseRecordIterator(RecordIterator iterator) {
    iterator->page = NULL;
    iterator->pageId = 1;
    iterator->slotIdx = 0;
}

Record iterateRecords(TableInfo tableInfo, Schema *schema,
                      RecordIterator recordIterator, bool autoClearPage) {
    // Checks if database is empty
    if (tableInfo->header->numPages == 0) {
        return NULL;
    }

    // Sets iterator fields at start of iteration
    if (recordIterator->page == NULL) {
        recordIterator->page = getPage(tableInfo, recordIterator->pageId);
        recordIterator->slotIdx = 0;
    }

    // Stores id of last page stored in memory
    const size_t maxId = tableInfo->header->numPages;

    while (recordIterator->pageId <= maxId) {
        // If end of slot array encountered, moves to next page
        if (recordIterator->slotIdx == recordIterator->page->header->slots.size) {
            recordIterator->pageId++;
            recordIterator->slotIdx = 0;

            // Frees previous page if not used elsewhere
            if (autoClearPage) {
                freePage(recordIterator->page);
            }

            continue;
        }
        // Reads next record slot
        RecordSlot *nextSlot =
            &recordIterator->page->header->slots.slots[recordIterator->slotIdx++];

        // Skips over empty slot
        if (nextSlot->size == 0) {
            continue;
        }

        recordIterator->lastSlot = nextSlot;
        return parseRecord(recordIterator->page->ptr + nextSlot->offset, schema);
    }

    return NULL;
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
