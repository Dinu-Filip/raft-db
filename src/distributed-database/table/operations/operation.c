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
#include "table/core/pages.h"
#include "table/core/record.h"
#include "table/core/recordArray.h"
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
    // Only set for RELATION - the other branches use static, shared Schemas
    // that must never be freed.
    Schema *heapSchema = NULL;

    if (tableType == RELATION) {
        char schemaName[100];
        snprintf(schemaName, sizeof(schemaName), "%s-schema", operation->tableName);
        TableInfo schemaInfo = openTable(schemaName);
        heapSchema = getSchema(schemaInfo);
        schema = *heapSchema;
        closeTable(schemaInfo);

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

    if (spaceInfo != NULL) {
        closeTable(spaceInfo);
    }
    closeTable(tableInfo);

    // The switch above was the last user of schema.attrInfos, shared with
    // heapSchema via the shallow copy above.
    if (heapSchema != NULL) {
        freeSchema(heapSchema);
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

static void freeOperand(Operand operand) {
    if (operand == NULL) return;
    if (operand->type == STR || operand->type == ATTR) {
        free(operand->value.strOp);
    }
    free(operand);
}

static void freeCondition(Condition condition) {
    if (condition == NULL) return;

    switch (condition->type) {
        case NOT:
            freeOperand(condition->value.oneArg.op1);
            break;
        case BETWEEN:
            freeOperand(condition->value.between.op1);
            freeOperand(condition->value.between.op2);
            freeOperand(condition->value.between.op3);
            break;
        default:
            // EQUALS/LESS_THAN/GREATER_THAN/LESS_EQUALS/GREATER_EQUALS/AND/OR
            // all share the twoArg (op1, op2) layout
            freeOperand(condition->value.twoArg.op1);
            freeOperand(condition->value.twoArg.op2);
            break;
    }
    free(condition);
}

static void freeQueryAttributes(QueryAttributes attributes) {
    if (attributes == NULL) return;
    for (int i = 0; i < attributes->numAttributes; i++) {
        free(attributes->attributes[i]);
    }
    free(attributes->attributes);
    free(attributes);
}

static void freeQueryValues(QueryValues values) {
    if (values == NULL) return;
    for (int i = 0; i < values->numValues; i++) {
        freeOperand(values->values[i]);
    }
    free(values->values);
    free(values);
}

static void freeQueryTypes(QueryTypes types) {
    if (types == NULL) return;
    for (int i = 0; i < types->numTypes; i++) {
        free(types->types[i]->name);
        free(types->types[i]);
    }
    free(types->types);
    free(types);
}

void freeOperation(Operation operation) {
    if (operation == NULL) return;

    switch (operation->queryType) {
        case SELECT:
            freeQueryAttributes(operation->query.select.attributes);
            freeCondition(operation->query.select.condition);
            break;
        case INSERT:
            freeQueryAttributes(operation->query.insert.attributes);
            freeQueryValues(operation->query.insert.values);
            break;
        case UPDATE:
            freeQueryAttributes(operation->query.update.attributes);
            freeQueryValues(operation->query.update.values);
            freeCondition(operation->query.update.condition);
            break;
        case DELETE:
            freeCondition(operation->query.delete.condition);
            break;
        case CREATE_TABLE:
            freeQueryTypes(operation->query.createTable.types);
            break;
    }

    free(operation->tableName);
    free(operation);
}

void freeQueryResult(QueryResult result) {
    if (result == NULL) return;
    freeRecordArray(result->records);
    free(result);
}
