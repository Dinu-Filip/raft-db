#include "schema.h"

#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "core/record.h"
#include "core/recordArray.h"
#include "log.h"
#include "operations/select.h"
#include "operations/sqlToOperation.h"
#include "table/core/table.h"

#define SCHEMA_RELATION_NAME "RELATION_NAME"
#define SCHEMA_IDX "SCHEMA_IDX"
#define SCHEMA_ATTRIBUTE_TYPE "ATTRIBUTE_TYPE"
#define SCHEMA_ATTRIBUTE_SIZE "ATTRIBUTE_SIZE"
#define SCHEMA_ATTRIBUTE_NAME "ATTRIBUTE_NAME"

#define ATTRIBUTE_SCHEMA_IDX 0
#define ATTRIBUTE_TYPE_IDX 1
#define ATTRIBUTE_SIZE_IDX 2
#define RELATION_NAME_IDX 3
#define ATTRIBUTE_NAME_IDX 4

#define NUM_SCHEMA_ATTRIBUTES 5

#define SPACE_PAGE_ID "PAGE_ID"
#define FREE_SPACE "FREE_SPACE"

#define PAGE_ID_IDX 0
#define FREE_SPACE_IDX 1

#define NUM_SPACE_ATTRIBUTES 2

static AttrInfo schemaAttrInfos[NUM_SCHEMA_ATTRIBUTES];
static Schema schema = {.attrInfos = schemaAttrInfos,
                        .numAttrs = NUM_SCHEMA_ATTRIBUTES};
static AttrInfo spaceAttrInfos[NUM_SPACE_ATTRIBUTES];
static Schema spaceSchema = {.attrInfos = spaceAttrInfos, .numAttrs = NUM_SPACE_ATTRIBUTES};

static int schemaRecordCompare(const void *record1, const void *record2) {
    // Comparator for sorting schema records on index
    Record r1 = *(Record *)record1;
    Record r2 = *(Record *)record2;
    int32_t idx1 = r1->fields[ATTRIBUTE_SCHEMA_IDX].intValue;
    int32_t idx2 = r2->fields[ATTRIBUTE_SCHEMA_IDX].intValue;

    if (idx1 < idx2) {
        return -1;
    }
    if (idx1 == idx2) {
        return 0;
    }
    return 1;
}

Schema getInventorySchema() {
    static bool initialised = false;

    if (initialised) {
        return spaceSchema;
    }

    spaceSchema.numAttrs = 2;

    AttrInfo *pageIdInfo = &spaceAttrInfos[PAGE_ID_IDX];
    pageIdInfo->type = INT;
    pageIdInfo->loc = INT_WIDTH * PAGE_ID_IDX;
    pageIdInfo->size = INT_WIDTH;
    pageIdInfo->name = SPACE_PAGE_ID;

    AttrInfo *freeSpaceInfo = &spaceAttrInfos[FREE_SPACE_IDX];
    freeSpaceInfo->type = INT;
    freeSpaceInfo->loc = INT_WIDTH * FREE_SPACE_IDX;
    freeSpaceInfo->size = INT_WIDTH;
    freeSpaceInfo->name = FREE_SPACE;

    initialised = true;
    return spaceSchema;
}

Schema getDictSchema() {
    static bool initialised = false;

    if (initialised) {
        return schema;
    }

    LOG("INIT DICT SCHEMA");
    schema.numAttrs = NUM_SCHEMA_ATTRIBUTES;

    AttrInfo *schemaIdxInfo = &schemaAttrInfos[ATTRIBUTE_SCHEMA_IDX];
    schemaIdxInfo->type = INT;
    schemaIdxInfo->loc = INT_WIDTH * ATTRIBUTE_SCHEMA_IDX;
    schemaIdxInfo->size = INT_WIDTH;
    schemaIdxInfo->name = SCHEMA_IDX;

    AttrInfo *schemaAttrTypeInfo = &schemaAttrInfos[ATTRIBUTE_TYPE_IDX];
    schemaAttrTypeInfo->type = INT;
    schemaAttrTypeInfo->loc = INT_WIDTH * ATTRIBUTE_TYPE_IDX;
    schemaAttrTypeInfo->size = INT_WIDTH;
    schemaAttrTypeInfo->name = SCHEMA_ATTRIBUTE_TYPE;

    AttrInfo *schemaAttrSizeInfo = &schemaAttrInfos[ATTRIBUTE_SIZE_IDX];
    schemaAttrSizeInfo->type = INT;
    schemaAttrSizeInfo->loc = INT_WIDTH * ATTRIBUTE_SIZE_IDX;
    schemaAttrSizeInfo->size = INT_WIDTH;
    schemaAttrSizeInfo->name = SCHEMA_ATTRIBUTE_SIZE;

    AttrInfo *relationNameInfo = &schemaAttrInfos[RELATION_NAME_IDX];
    relationNameInfo->type = VARSTR;
    relationNameInfo->loc = 0;
    relationNameInfo->size = MAX_TABLE_NAME_LEN;
    relationNameInfo->name = SCHEMA_RELATION_NAME;

    AttrInfo *attributeNameInfo = &schemaAttrInfos[ATTRIBUTE_NAME_IDX];
    attributeNameInfo->type = VARSTR;
    attributeNameInfo->loc = 1;
    attributeNameInfo->size = MAX_ATTRIBUTE_NAME;
    attributeNameInfo->name = SCHEMA_ATTRIBUTE_NAME;

    initialised = true;
    return schema;
}

Schema *getSchema(TableInfo schemaInfo) {
    Schema *schema = malloc(sizeof(Schema));
    assert(schema != NULL);

    char template[] = "select * from %s";
    char sql[100];
    snprintf(sql, sizeof(sql), template, schemaInfo->name);

    Schema dictSchema = getDictSchema();

    // Retrieves records from schema store for this table
    QueryResult result =
        selectOperation(schemaInfo, &dictSchema, sqlToOperation(sql));

    unsigned numRecords = result->records->size;

    // Sorts schema records based on index in schema
    qsort(result->records->records, numRecords, sizeof(Record),
          schemaRecordCompare);

    schema->attrInfos = malloc(sizeof(AttrInfo) * numRecords);
    assert(schema->attrInfos != NULL);

    unsigned staticLoc = 0;
    unsigned varLoc = 0;

    // Parses records into schema, maintaining order of attributes in original
    // schema
    for (int i = 0; i < numRecords; i++) {
        Record record = result->records->records[i];
        AttrInfo *info =
            &schema->attrInfos[record->fields[ATTRIBUTE_SCHEMA_IDX].intValue];

        info->name = strdup(record->fields[ATTRIBUTE_NAME_IDX].stringValue);
        info->type = record->fields[ATTRIBUTE_TYPE_IDX].intValue;
        info->size = record->fields[ATTRIBUTE_SIZE_IDX].intValue;

        if (info->type == VARSTR) {
            info->loc = varLoc++;
        } else {
            info->loc = staticLoc;
            staticLoc += info->size;
        }
    }

    freeRecordArray(result->records);
    free(result);

    schema->numAttrs = numRecords;
    return schema;
}

void freeSchema(Schema *schema) {
    for (int i = 0; i < schema->numAttrs; i++) {
        free(schema->attrInfos[i].name);
    }
    free(schema->attrInfos);
    free(schema);
}
