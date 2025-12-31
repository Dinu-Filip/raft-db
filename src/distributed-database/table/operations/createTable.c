#include "createTable.h"

#include <assert.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>

#include "../core/field.h"
#include "../schema.h"
#include "insert.h"
#include "sqlToOperation.h"
#include "table/core/table.h"

static void setSchema(char *tableName, QueryTypes descriptors) {
    // Creates schema table
    char schemaName[MAX_FILE_NAME_LEN];
    snprintf(schemaName, MAX_FILE_NAME_LEN, "%s-schema", tableName);
    initialiseTable(schemaName);

    TableInfo schemaInfo = openTable(schemaName);

    Schema dictSchema = getDictSchema();

    // Insertion command template
    char template[] = "insert into %s values (%d, %d, %d, %s, %s);";
    char sql[500];

    unsigned numTypes = descriptors->numTypes;

    // Inserts fixed-length fields with index
    for (int i = 0; i < numTypes; i++) {
        QueryTypeDescriptor type = descriptors->types[i];
        snprintf(sql, sizeof(sql), template, schemaName, i, type->type, type->size, tableName, type->name);
        insertOperation(schemaInfo, NULL, &dictSchema, sqlToOperation(sql), SCHEMA);
    }

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
