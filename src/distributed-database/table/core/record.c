#include "record.h"

#include <assert.h>
#include <string.h>

#include "log.h"

static void parseQueryValueToField(Field *field, Operand attributeValue,
                                   Attribute attributeName,
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

static void setGlobalIdx(Record record, uint32_t globalIdx) {
    assert(record->fields != NULL);

    record->fields[GLOBAL_ID_RECORD_IDX].attribute = strdup(GLOBAL_ID_NAME);
    record->fields[GLOBAL_ID_RECORD_IDX].type = INT;

    assert(globalIdx <= INT32_MAX);
    record->fields[GLOBAL_ID_RECORD_IDX].uintValue = globalIdx;
    record->fields[GLOBAL_ID_RECORD_IDX].size = GLOBAL_ID_WIDTH;
}

Record parseQuery(Schema *schema, QueryAttributes attributes,
                  QueryValues values, uint32_t globalIdx) {
    // Parses query into internal representation

    LOG("PARSE QUERY\n");
    Record record = malloc(sizeof(struct Record));
    assert(record != NULL);

    // Allocates array of fields of record
    record->fields = malloc(sizeof(Field) * (schema->numAttributes + 1));
    assert(record->fields != NULL);

    // Computes the size of the record data in bytes
    unsigned recordSize = 0;

    // Sets global index and adds to record size
    setGlobalIdx(record, globalIdx);
    recordSize += GLOBAL_ID_WIDTH;

    // Iterates over query, matching schema to attributes
    record->globalIdx = globalIdx;
    for (int j = 0; j < schema->numAttributes; j++) {
        for (int i = 0; i < attributes->numAttributes; i++) {
            Attribute attributeName = attributes->attributes[i];

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
    recordSize += RECORD_IDX;

    record->size = recordSize;

    // Number of attribute values including global index
    record->numValues = attributes->numAttributes + 1;

    LOG("RECORD PARSE SUCCESSFUL");
    return record;
}
