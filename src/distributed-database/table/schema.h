#ifndef SCHEMA_H
#define SCHEMA_H

#include "table/operation.h"

#define MAX_RELATION_NAME 20
#define MAX_ATTRIBUTE_NAME 20

#define SCHEMA_RELATION_NAME "RELATION_NAME"
#define SCHEMA_ATTRIBUTE_TYPE "ATTRIBUTE_TYPE"
#define SCHEMA_IDX "IDX"
#define SCHEMA_ATTRIBUTE_SIZE "ATTRIBUTE_SIZE"
#define SCHEMA_ATTRIBUTE_NAME "ATTRIBUTE_NAME"

#define SPACE_INFO_RELATION "RELATION_NAME"
#define SPACE_INFO_ID "PAGE_ID"
#define SPACE_INFO_FREE_SPACE "FREE_SPACE"

#define RELATION_NAME_IDX 0
#define ATTRIBUTE_TYPE_IDX 1
#define ATTRIBUTE_IDX_IDX 2
#define ATTRIBUTE_SIZE_IDX 3
#define ATTRIBUTE_NAME_IDX 4

#define RELATION_NAME_IDX 0
#define SPACE_ID_IDX 1
#define SPACE_FREE_IDX 2

#define NUM_SCHEMA_ATTRIBUTES 5
#define NUM_SPACE_INFO_ATTRIBUTES 3

typedef struct TableInfo *TableInfo;

typedef struct Schema Schema;
struct Schema {
    AttributeName *attributes;
    AttributeType *attributeTypes;
    unsigned int *attributeSizes;
    int numAttributes;
};

extern AttributeName schemaAttributes[5];
extern AttributeType schemaTypes[5];
extern unsigned int schemaSize[5];
extern AttributeName spaceInfoAttributes[3];
extern AttributeType spaceInfoTypes[3];
extern unsigned int spaceInfoSizes[3];

/**
 * Retrieves and parses schema from table
 * @param schemaInfo schema table
 * @param tableName name of relation table
 */
extern Schema *getSchema(TableInfo schemaInfo, char *tableName);

/**
 * Initialises schema for space inventory
 * @param tableName
 */
extern Schema *initSpaceInfoSchema(char *tableName);

/**
 * Initialises schema for relation schema
 */
extern Schema *initDictSchema();

/**
 * Initialises attributes for space inventory
 */
extern QueryAttributes initSpaceInfoQueryAttributes();

/**
 * Frees schema
 * @param schema
 */
extern void freeSchema(Schema *schema);

#endif  // SCHEMA_H
