#ifndef SCHEMA_H
#define SCHEMA_H

#include "operations/operation.h"

#define MAX_RELATION_NAME 20
#define MAX_ATTRIBUTE_NAME 20

typedef struct TableInfo *TableInfo;
typedef char *AttributeName;


typedef struct AttrInfo AttrInfo;
struct AttrInfo {
    AttributeName name; // Name of attribute
    AttributeType type; // Data type of attribute
    unsigned size;      /* Size of attribute in bytes (calculated internally
                           if static length or provided on table creation for
                           strings and variable length fields */
    unsigned loc;       /* Offset from static field start for static fields
                           or slot index */
};

typedef struct Schema Schema;
struct Schema {
    unsigned numAttrs;
    AttrInfo *attrInfos; // Ordered based on record
};

/**
 * Retrieves and parses schema from table
 * @param schemaInfo schema table
 * @param tableName name of relation table
 */
extern Schema *getSchema(TableInfo schemaInfo, char *tableName);

/**
 * Initialises schema for relation schema
 */
extern Schema getDictSchema();

/**
 * Frees schema
 * @param schema
 */
extern void freeSchema(Schema *schema);

#endif  // SCHEMA_H
