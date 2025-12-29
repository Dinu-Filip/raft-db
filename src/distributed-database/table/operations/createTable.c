#include "createTable.h"

#include <assert.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>

#include "../core/field.h"
#include "../schema.h"
#include "insert.h"
#include "log.h"
#include "sqlToOperation.h"
#include "table/core/table.h"

static void setSchema(char *tableName, QueryTypes descriptors) {
    // Creates schema table
    char schemaName[MAX_FILE_NAME_LEN];
    snprintf(schemaName, MAX_FILE_NAME_LEN, "%s-schema", tableName);
    initialiseTable(schemaName);

    TableInfo schemaInfo = openTable(schemaName);

    // Gets data dictionary schema
    Schema *dictSchema = initDictSchema();

    // Set length of relation name to length of table name
    dictSchema->attributeSizes[RELATION_NAME_IDX] = strlen(tableName);

    // Insertion command template
    char template[] = "insert into %s values (%s, %d, %d, %d, %s);";
    char sql[500];

    // Index of attribute in schema
    int idx = 0;

    unsigned numTypes = descriptors->numTypes;

    // Inserts fixed-length fields with index
    for (int i = 0; i < numTypes; i++) {
        QueryTypeDescriptor type = descriptors->types[i];
        if (type->type != VARSTR) {
            snprintf(sql, sizeof(sql), template, schemaName, type->type, idx, type->size, type->name);
            insertOperation(schemaInfo, NULL, dictSchema, sqlToOperation(sql), SCHEMA);
            idx++;
        }
    }

    // Inserts variable-length fields with index
    for (int i = 0; i < numTypes; i++) {
        QueryTypeDescriptor type = descriptors->types[i];
        if (type->type == VARSTR) {
            snprintf(sql, sizeof(sql), template, schemaName, type->type, idx, type->size, type->name);
            insertOperation(schemaInfo, NULL, dictSchema, sqlToOperation(sql), SCHEMA);
            idx++;
        }
    }

    free(dictSchema);
    closeTable(schemaInfo);
}

void createTable(Operation operation) {
    initialiseTable(operation->tableName);

    unsigned numTypes = operation->query.createTable.types->numTypes;

    // Sets size of static non-string fields
    for (int i = 0; i < numTypes; i++) {
        QueryTypeDescriptor type = operation->query.createTable.types->types[i];
        if (type->type != VARSTR && type->type != STR) {
            type->size = getStaticTypeWidth(type->type);
        }
    }

    // Writes the schema of the table
    setSchema(operation->tableName, operation->query.createTable.types);

    char *spaceTableName[MAX_FILE_NAME_LEN];
    snprintf(spaceTableName, MAX_FILE_NAME_LEN, "%s-space-inventory",
             operation->tableName);

    initialiseTable(spaceTableName);
}
