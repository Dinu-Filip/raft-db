#include "schema.h"

#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "core/record.h"
#include "core/recordArray.h"
#include "log.h"
#include "select.h"
#include "table/core/table.h"

AttributeName schemaAttributes[NUM_SCHEMA_ATTRIBUTES] = {
    SCHEMA_RELATION_NAME, SCHEMA_ATTRIBUTE_TYPE, SCHEMA_IDX,
    SCHEMA_ATTRIBUTE_SIZE, SCHEMA_ATTRIBUTE_NAME};
AttributeType schemaTypes[NUM_SCHEMA_ATTRIBUTES] = {STR, INT, INT, INT, VARSTR};
unsigned int schemaSize[NUM_SCHEMA_ATTRIBUTES] = {
    MAX_RELATION_NAME, INT_WIDTH, INT_WIDTH, INT_WIDTH, MAX_ATTRIBUTE_NAME};
AttributeName spaceInfoAttributes[NUM_SPACE_INFO_ATTRIBUTES] = {
    SPACE_INFO_RELATION, SPACE_INFO_ID, SPACE_INFO_FREE_SPACE};
AttributeType spaceInfoTypes[NUM_SPACE_INFO_ATTRIBUTES] = {STR, INT, INT};
unsigned int spaceInfoSizes[NUM_SPACE_INFO_ATTRIBUTES] = {MAX_RELATION_NAME,
                                                          INT_WIDTH, INT_WIDTH};

static int schemaRecordCompare(const void *record1, const void *record2) {
    // Comparator for sorting schema records on index
    Record r1 = *(Record *)record1;
    Record r2 = *(Record *)record2;
    int32_t idx1 = r1->fields[ATTRIBUTE_IDX_IDX].intValue;
    int32_t idx2 = r2->fields[ATTRIBUTE_IDX_IDX].intValue;

    if (idx1 < idx2) {
        return -1;
    }
    if (idx1 == idx2) {
        return 0;
    }
    return 1;
}

QueryAttributes initSchemaQueryAttributes() {
    QueryAttributes queryAttributes = malloc(sizeof(struct QueryAttributes));
    assert(queryAttributes != NULL);

    AttributeName schemaAttributes[] = {SCHEMA_RELATION_NAME, SCHEMA_ATTRIBUTE_TYPE,
                                    SCHEMA_IDX, SCHEMA_ATTRIBUTE_SIZE,
                                    SCHEMA_ATTRIBUTE_NAME};

    queryAttributes->attributes = schemaAttributes;
    queryAttributes->numAttributes = NUM_SCHEMA_ATTRIBUTES;

    return queryAttributes;
}

QueryAttributes initSpaceInfoQueryAttributes() {
    QueryAttributes queryAttributes = malloc(sizeof(struct QueryAttributes));
    assert(queryAttributes != NULL);

    queryAttributes->attributes = spaceInfoAttributes;
    queryAttributes->numAttributes = NUM_SPACE_INFO_ATTRIBUTES;

    return queryAttributes;
}

Schema *initDictSchema() {
    LOG("INIT DICT SCHEMA");

    Schema *schema = malloc(sizeof(Schema));
    assert(schema != NULL);

    schema->attributes = schemaAttributes;
    schema->attributeTypes = schemaTypes;
    schema->attributeSizes = schemaSize;
    schema->numAttributes = NUM_SCHEMA_ATTRIBUTES;

    return schema;
}

Schema *initSpaceInfoSchema(char *tableName) {
    Schema *schema = malloc(sizeof(Schema));
    assert(schema != NULL);

    schema->attributes = spaceInfoAttributes;
    schema->attributeTypes = spaceInfoTypes;
    schema->attributeSizes = spaceInfoSizes;
    schema->numAttributes = NUM_SPACE_INFO_ATTRIBUTES;

    schema->attributeSizes[RELATION_NAME_IDX] = strlen(tableName);

    return schema;
}

Schema *getSchema(TableInfo schemaInfo, char *tableName) {
    Schema *schema = malloc(sizeof(Schema));
    assert(schema != NULL);

    // Creates select operation to get records from schema table
    struct Condition condition;
    condition.type = EQUALS;
    AttributeName relationName = SCHEMA_RELATION_NAME;
    condition.value.twoArg.op1 = relationName;
    condition.value.twoArg.op2 = createOperand(STR, tableName);

    QueryAttributes dictAttributes = createQueryAttributes(
        NUM_SCHEMA_ATTRIBUTES, SCHEMA_RELATION_NAME, SCHEMA_ATTRIBUTE_TYPE,
        SCHEMA_IDX, SCHEMA_ATTRIBUTE_SIZE, SCHEMA_ATTRIBUTE_NAME);
    Schema *dictSchema = initDictSchema();
    dictSchema->attributeSizes[RELATION_NAME_IDX] = strlen(tableName);
    QueryResult result =
        selectFrom(schemaInfo, dictSchema, &condition, dictAttributes);

    LOG("Checking schema record output");
    LOG("Num records: %d\n", result->numRecords);

    // Sorts schema records based on index in schema
    qsort(result->records->records, result->numRecords, sizeof(Record),
          schemaRecordCompare);

    schema->attributes = malloc(sizeof(uint8_t *) * result->numRecords);
    assert(schema->attributes != NULL);

    schema->attributeTypes = malloc(sizeof(uint8_t *) * result->numRecords);
    assert(schema->attributeTypes != NULL);

    schema->attributeSizes = malloc(sizeof(uint8_t *) * result->numRecords);
    assert(schema->attributeSizes != NULL);

    // Parses records into schema, maintaining order of attributes in original
    // schema
    for (int i = 0; i < result->numRecords; i++) {
        Record record = result->records->records[i];
        schema->attributes[i] =
            strdup(record->fields[ATTRIBUTE_NAME_IDX].stringValue);
        Field field = record->fields[ATTRIBUTE_TYPE_IDX];
        schema->attributeTypes[i] = field.intValue;
        schema->attributeSizes[i] = record->fields[ATTRIBUTE_SIZE_IDX].intValue;
    }

    schema->numAttributes = result->numRecords;

    freeRecordArray(result->records);
    free(result);
    free(dictSchema);
    freeQueryAttributes(dictAttributes);
    free(condition.value.twoArg.op2->value.strOp);
    free(condition.value.twoArg.op2);

    return schema;
}

void freeSchema(Schema *schema) {
    for (int i = 0; i < schema->numAttributes; i++) {
        free(schema->attributes[i]);
    }
    free(schema->attributes);
    free(schema->attributeSizes);
    free(schema->attributeTypes);
    free(schema);
}
