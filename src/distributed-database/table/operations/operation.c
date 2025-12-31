#include "operation.h"

#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "../schema.h"
#include "createTable.h"
#include "delete.h"
#include "insert.h"
#include "log.h"
#include "select.h"
#include "table/core/table.h"
#include "update.h"

QueryResult executeQualifiedOperation(Operation operation, TableType tableType) {
    if (operation->queryType == CREATE_TABLE) {
        createTable(operation);
        return NULL;
    }

    TableInfo tableInfo = openTable(operation->tableName);
    Schema schema;
    TableInfo spaceInfo = NULL;

    if (tableType == RELATION) {
        char schemaName[100];
        snprintf(schemaName, sizeof(schemaName), "%s-schema", operation->tableName);
        TableInfo schemaInfo = openTable(schemaName);
        schema = *getSchema(schemaInfo, operation->tableName);

        char spaceName[100];
        snprintf(spaceName, sizeof(spaceName), "%s-space-inventory", operation->tableName);
        spaceInfo = openTable(spaceName);
    } else if (tableType == SCHEMA) {
        Schema dictSchema = getDictSchema();
        schema = dictSchema;
    } else {
        Schema spaceSchema = getInventorySchema();
        schema = spaceSchema;
    }

    QueryResult res = NULL;

    switch (operation->queryType) {
        case SELECT:
            res = selectOperation(tableInfo, &schema, operation);
            break;
        case INSERT:
            insertOperation(tableInfo, spaceInfo, &schema, operation, tableType);
            break;
        case UPDATE:
            updateOperation(tableInfo, spaceInfo, &schema, operation);
            break;
        case DELETE:
            deleteOperation(tableInfo, spaceInfo, &schema, operation);
            break;
        default:
            LOG_ERROR("Unexpected operation\n");
    }

    return res;
}

QueryResult executeOperation(Operation operation) {
    return executeQualifiedOperation(operation, RELATION);
}

void initDatabasePath(size_t nodeId) {
    int pathLen = snprintf(DB_DIRECTORY, MAX_FILE_NAME_LEN, "%s/%ld/data",
                           DB_BASE_DIRECTORY, nodeId);
    assert(pathLen < MAX_FILE_NAME_LEN);

    LOG("Database path: %s/%ld/data", DB_BASE_DIRECTORY, nodeId);
}

bool isWriteOperation(Operation operation) {
    return operation->queryType != SELECT;
}
