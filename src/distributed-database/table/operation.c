#include "operation.h"

#include <assert.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "createTable.h"
#include "delete.h"
#include "insert.h"
#include "log.h"
#include "schema.h"
#include "select.h"
#include "table/table.h"
#include "update.h"

QueryResult executeOperation(Operation operation) {
    if (operation->queryType == CREATE_TABLE) {
        createTable(operation);
        return NULL;
    }

    TableInfo relationInfo = openTable(operation->tableName);

    char buffer[MAX_FILE_NAME_LEN];
    snprintf(buffer, MAX_FILE_NAME_LEN, "%s-schema", operation->tableName);
    TableInfo schemaInfo = openTable(buffer);

    snprintf(buffer, MAX_FILE_NAME_LEN, "%s-space-inventory",
             operation->tableName);
    TableInfo spaceInventory = openTable(buffer);

    Schema *schema = getSchema(schemaInfo, operation->tableName);
    QueryResult res = NULL;

    switch (operation->queryType) {
        case SELECT:
            res = selectOperation(relationInfo, schema, operation);
            break;
        case INSERT:
            insertOperation(relationInfo, spaceInventory, schema, operation);
            break;
        case UPDATE:
            updateOperation(relationInfo, spaceInventory, schema, operation);
            break;
        case DELETE:
            deleteOperation(relationInfo, spaceInventory, schema, operation);
            break;
        default:
            LOG_ERROR("Unexpected operation\n");
    }
    closeTable(relationInfo);
    closeTable(schemaInfo);
    closeTable(spaceInventory);
    freeSchema(schema);
    return res;
}

Operation createSelectTwoArgOperation() {
    Operation operation = malloc(sizeof(struct Operation));
    assert(operation != NULL);

    operation->queryType = SELECT;
    operation->query.select.condition = malloc(sizeof(struct Condition));
    assert(operation->query.select.condition != NULL);

    operation->query.select.condition->value.twoArg.op2 =
        malloc(sizeof(struct Operand));
    assert(operation->query.select.condition->value.twoArg.op2 != NULL);

    return operation;
}

void freeSelectTwoArgOperation(Operation operation) {
    free(operation->query.select.condition);
    free(operation->query.select.condition->value.twoArg.op2);
    free(operation);
}

Operation createInsertOperation() {
    Operation operation = malloc(sizeof(struct Operation));
    assert(operation != NULL);

    operation->queryType = INSERT;
    return operation;
}

void freeInsertOperationTest(Operation operation) {
    freeQueryAttributes(operation->query.insert.attributes);
    freeQueryValues(operation->query.insert.values);
}

QueryAttributes createQueryAttributes(size_t numAttributes, ...) {
    QueryAttributes queryAttributes = malloc(sizeof(struct QueryAttributes));
    assert(queryAttributes != NULL);

    queryAttributes->attributes = malloc(sizeof(AttributeName) * numAttributes);
    assert(queryAttributes->attributes != NULL);

    va_list attributes;
    va_start(attributes, numAttributes);
    for (size_t i = 0; i < numAttributes; i++) {
        queryAttributes->attributes[i] = va_arg(attributes, AttributeName);
    }
    va_end(attributes);
    queryAttributes->numAttributes = numAttributes;
    return queryAttributes;
}

void freeQueryAttributes(QueryAttributes attributes) {
    free(attributes->attributes);
    free(attributes);
}

Operand createOperand(AttributeType type, void *value) {
    Operand operand = malloc(sizeof(struct Operand));
    assert(operand != NULL);

    switch (type) {
        case INT:
            operand->value.intOp = *(int *)value;
            break;
        case FLOAT:
            operand->value.floatOp = *(float *)value;
            break;
        case BOOL:
            operand->value.boolOp = *(int *)value;
            break;
        case STR:
        case VARSTR:
            operand->value.strOp = strdup(value);
        default:
            break;
    }
    operand->type = type;
    return operand;
}

QueryValues createQueryValues(size_t numValues, ...) {
    QueryValues queryValues = malloc(sizeof(struct QueryValues));
    assert(queryValues != NULL);

    queryValues->values = malloc(sizeof(Operand) * numValues);
    assert(queryValues->values != NULL);

    va_list values;
    va_start(values, numValues);
    for (size_t i = 0; i < numValues; i++) {
        queryValues->values[i] = va_arg(values, Operand);
    }
    va_end(values);
    queryValues->numValues = numValues;
    return queryValues;
}

void freeQueryValues(QueryValues queryValues) {
    for (int i = 0; i < queryValues->numValues; i++) {
        // Frees duplicated string for STR/VARSTR types
        if (queryValues->values[i]->type == VARSTR ||
            queryValues->values[i]->type == STR) {
            free(queryValues->values[i]->value.strOp);
        }
        free(queryValues->values[i]);
    }
    free(queryValues->values);
    free(queryValues);
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
