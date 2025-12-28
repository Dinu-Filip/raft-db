#include "client-handling/input.h"

#include <assert.h>
#include <stdio.h>
#include <string.h>
#include <third-party/cJSON.h>

#include "log.h"
#include "table/core/record.h"
#include "table/core/recordArray.h"
#include "table/core/table.h"

static int getJsonArrayLength(cJSON *array) {
    int i = 0;

    cJSON *element = NULL;
    cJSON_ArrayForEach(element, array) { i++; }

    return i;
}

// JSON to Operation

static QueryAttributes parseQueryAttributes(cJSON *operationJson) {
    cJSON *attributes =
        cJSON_GetObjectItemCaseSensitive(operationJson, "attributes");

    int attributesLength = getJsonArrayLength(attributes);

    QueryAttributes attrs = malloc(sizeof(struct QueryAttributes));
    assert(attrs != NULL);

    attrs->numAttributes = attributesLength;
    attrs->attributes = malloc(sizeof(AttributeName) * attrs->numAttributes);
    assert(attrs->attributes != NULL);

    cJSON *attr;
    int i = 0;
    cJSON_ArrayForEach(attr, attributes) {
        if (!cJSON_IsString(attr)) {
            for (int j = 0; j < i; j++) {
                free(attrs->attributes[j]);
            }
            free(attrs->attributes);
            free(attrs);
            return NULL;
        }

        attrs->attributes[i] = strdup(attr->valuestring);
        i++;
    }

    return attrs;
}

static Operand parseOperand(cJSON *value) {
    Operand operand = malloc(sizeof(struct Operand));
    assert(operand != NULL);

    if (cJSON_IsNumber(value) &&
        ((double)value->valueint) == value->valuedouble) {
        operand->type = INT;
        operand->value.intOp = value->valueint;
    } else if (cJSON_IsNumber(value)) {
        operand->type = FLOAT;
        operand->value.floatOp = value->valuedouble;
    } else if (cJSON_IsString(value)) {
        operand->type = STR;
        operand->value.strOp = strdup(value->valuestring);
    } else if (cJSON_IsBool(value)) {
        operand->type = BOOL;
        operand->value.boolOp = cJSON_IsTrue(value) ? true : false;
    } else {
        free(operand);
        return NULL;
    }

    return operand;
}

static QueryValues parseQueryValues(cJSON *operationJson) {
    cJSON *values = cJSON_GetObjectItemCaseSensitive(operationJson, "values");

    int valuesLength = getJsonArrayLength(values);

    QueryValues queryValues = malloc(sizeof(struct QueryValues));
    assert(queryValues != NULL);

    queryValues->numValues = valuesLength;
    queryValues->values = malloc(sizeof(Operand) * queryValues->numValues);
    assert(queryValues->values);

    cJSON *v;
    int i = 0;
    cJSON_ArrayForEach(v, values) {
        queryValues->values[i] = parseOperand(v);

        if (queryValues->values[i] == NULL) {
            for (int j = 0; j < i; j++) {
                free(queryValues->values[j]);
            }
            free(queryValues->values);
            free(queryValues);
            return NULL;
        }

        i++;
    }

    return queryValues;
}

static QueryTypes parseQueryTypes(cJSON *operationJson) {
    cJSON *attributeTypes =
        cJSON_GetObjectItemCaseSensitive(operationJson, "types");
    cJSON *sizes = cJSON_GetObjectItemCaseSensitive(operationJson, "sizes");

    QueryTypes queryTypes = malloc(sizeof(struct QueryTypes));
    assert(queryTypes != NULL);

    int typesLength = getJsonArrayLength(attributeTypes);
    queryTypes->numTypes = typesLength;
    queryTypes->types = malloc(sizeof(AttributeType) * queryTypes->numTypes);
    assert(queryTypes != NULL);

    cJSON *element;
    int i = 0;
    cJSON_ArrayForEach(element, attributeTypes) {
        if (!cJSON_IsString(element)) {
            free(queryTypes->types);
            free(queryTypes);
            return NULL;
        }

        if (strcmp(element->valuestring, "INT") == 0) {
            queryTypes->types[i] = INT;
        } else if (strcmp(element->valuestring, "STR") == 0) {
            queryTypes->types[i] = STR;
        } else if (strcmp(element->valuestring, "VARSTR") == 0) {
            queryTypes->types[i] = VARSTR;
        } else if (strcmp(element->valuestring, "FLOAT") == 0) {
            queryTypes->types[i] = FLOAT;
        } else if (strcmp(element->valuestring, "BOOL") == 0) {
            queryTypes->types[i] = BOOL;
        } else {
            LOG("Invalid type passed in for query type");
            free(queryTypes->types);
            free(queryTypes);
            return NULL;
        }

        i++;
    }

    int sizesLength = getJsonArrayLength(sizes);
    // queryTypes->numSizes = sizesLength;
    // queryTypes->sizes = malloc(sizeof(size_t) * queryTypes->numSizes);

    i = 0;
    cJSON_ArrayForEach(element, sizes) {
        if (!cJSON_IsNumber(element)) {
            free(queryTypes->types);
            free(queryTypes);
            return NULL;
        }

        // queryTypes->sizes[i] = element->valueint;

        i++;
    }

    return queryTypes;
}

static Condition parseOneArgCondition(cJSON *conditionJson) {
    cJSON *op1 = cJSON_GetObjectItemCaseSensitive(conditionJson, "op1");
    if (!cJSON_IsString(op1)) {
        return NULL;
    }

    Condition condition = malloc(sizeof(struct Condition));
    assert(condition != NULL);

    condition->value.oneArg.op1->type = ATTR;
    condition->value.oneArg.op1->value.strOp = strdup(op1->valuestring);

    return condition;
}

static Condition parseTwoArgCondition(cJSON *conditionJson) {
    cJSON *op1 = cJSON_GetObjectItemCaseSensitive(conditionJson, "op1");
    if (!cJSON_IsString(op1)) {
        return NULL;
    }
    cJSON *op2 = cJSON_GetObjectItemCaseSensitive(conditionJson, "op2");
    Operand op2Operand = parseOperand(op2);
    if (op2Operand == NULL) {
        return NULL;
    }

    Condition condition = malloc(sizeof(struct Condition));
    assert(condition != NULL);

    condition->value.twoArg.op1->type = ATTR;
    condition->value.twoArg.op1->value.strOp = strdup(op1->valuestring);
    condition->value.twoArg.op2 = op2Operand;

    return condition;
}

static Condition parseThreeArgCondition(cJSON *conditionJson) {
    cJSON *op1 = cJSON_GetObjectItemCaseSensitive(conditionJson, "op1");
    if (!cJSON_IsString(op1)) {
        return NULL;
    }
    cJSON *op2 = cJSON_GetObjectItemCaseSensitive(conditionJson, "op2");
    Operand op2Operand = parseOperand(op2);
    if (op2Operand == NULL) {
        return NULL;
    }
    cJSON *op3 = cJSON_GetObjectItemCaseSensitive(conditionJson, "op3");
    Operand op3Operand = parseOperand(op3);
    if (op3Operand == NULL) {
        return NULL;
    }

    Condition condition = malloc(sizeof(struct Condition));
    assert(condition != NULL);

    condition->value.between.op1->type = ATTR;
    condition->value.between.op1->value.strOp = strdup(op1->valuestring);
    condition->value.between.op2 = op2Operand;
    condition->value.between.op3 = op3Operand;

    return condition;
}

static Condition parseCondition(cJSON *operationJson) {
    cJSON *conditionJson =
        cJSON_GetObjectItemCaseSensitive(operationJson, "condition");
    if (conditionJson == NULL) {
        LOG("Parse condition did not have a condition object passed into it");
        return NULL;
    }

    cJSON *type = cJSON_GetObjectItemCaseSensitive(conditionJson, "type");
    if (!cJSON_IsString(type)) {
        LOG("Parse condition did not have a type passed into it");
        return NULL;
    }

    if (strcmp(type->valuestring, "EQUALS") == 0) {
        Condition condition = parseTwoArgCondition(conditionJson);
        condition->type = EQUALS;
        return condition;
    } else if (strcmp(type->valuestring, "LESS_THAN") == 0) {
        Condition condition = parseTwoArgCondition(conditionJson);
        condition->type = LESS_THAN;
        return condition;
    } else if (strcmp(type->valuestring, "GREATER_THAN") == 0) {
        Condition condition = parseTwoArgCondition(conditionJson);
        condition->type = GREATER_THAN;
        return condition;
    } else if (strcmp(type->valuestring, "AND") == 0) {
        Condition condition = parseTwoArgCondition(conditionJson);
        condition->type = AND;
        return condition;
    } else if (strcmp(type->valuestring, "OR") == 0) {
        Condition condition = parseTwoArgCondition(conditionJson);
        condition->type = OR;
        return condition;
    } else if (strcmp(type->valuestring, "LESS_EQUALS") == 0) {
        Condition condition = parseTwoArgCondition(conditionJson);
        condition->type = LESS_EQUALS;
        return condition;
    } else if (strcmp(type->valuestring, "GREATER_EQUALS") == 0) {
        Condition condition = parseTwoArgCondition(conditionJson);
        condition->type = GREATER_EQUALS;
        return condition;
    } else if (strcmp(type->valuestring, "NOT") == 0) {
        Condition condition = parseOneArgCondition(conditionJson);
        condition->type = NOT;
        return condition;
    } else if (strcmp(type->valuestring, "BETWEEN") == 0) {
        Condition condition = parseThreeArgCondition(conditionJson);
        condition->type = BETWEEN;
        return condition;
    } else {
        LOG("Type in condition did not have a valid type it was: %s",
            type->valuestring);
        return NULL;
    }
}

static Operation parseSelectOperation(Operation operation,
                                      cJSON *operationJson) {
    operation->queryType = SELECT;
    operation->query.select.attributes = parseQueryAttributes(operationJson);
    if (operation->query.select.attributes == NULL) {
        free(operation);

        return NULL;
    }

    operation->query.select.condition = parseCondition(operationJson);

    return operation;
}

static Operation parseInsertOperation(Operation operation,
                                      cJSON *operationJson) {
    operation->queryType = INSERT;

    operation->query.insert.attributes = parseQueryAttributes(operationJson);
    if (operation->query.insert.attributes == NULL) {
        free(operation);

        return NULL;
    }

    operation->query.insert.values = parseQueryValues(operationJson);
    if (operation->query.insert.values == NULL) {
        // TODO free attributes
        free(operation);

        return NULL;
    }

    return operation;
}

static Operation parseUpdateOperation(Operation operation,
                                      cJSON *operationJson) {
    operation->queryType = UPDATE;

    operation->query.update.attributes = parseQueryAttributes(operationJson);
    if (operation->query.update.attributes == NULL) {
        free(operation);

        return NULL;
    }

    operation->query.update.values = parseQueryValues(operationJson);
    if (operation->query.update.values == NULL) {
        // TODO free attributes
        free(operation);

        return NULL;
    }

    operation->query.update.condition = parseCondition(operationJson);

    return operation;
}

static Operation parseDeleteOperation(Operation operation,
                                      cJSON *operationJson) {
    operation->queryType = DELETE;

    operation->query.delete.condition = parseCondition(operationJson);

    return operation;
}

static Operation parseCreateTableOperation(Operation operation,
                                           cJSON *operationJson) {
    operation->queryType = CREATE_TABLE;

    // operation->query.createTable.attributes =
    //     parseQueryAttributes(operationJson);
    // if (operation->query.createTable.attributes == NULL) {
    //     free(operation);
    //
    //     return NULL;
    // }

    operation->query.createTable.types = parseQueryTypes(operationJson);
    if (operation->query.createTable.types == NULL) {
        // TODO free attributes
        free(operation);

        return NULL;
    }

    return operation;
}

Operation parseOperationJson(const char *jsonString) {
    cJSON *operationJson = cJSON_Parse(jsonString);
    if (operationJson == NULL) {
        cJSON_Delete(operationJson);
        return NULL;
    }

    const cJSON *queryType =
        cJSON_GetObjectItemCaseSensitive(operationJson, "queryType");
    if (!cJSON_IsString(queryType)) {
        cJSON_Delete(operationJson);
        return NULL;
    }

    const cJSON *tableName =
        cJSON_GetObjectItemCaseSensitive(operationJson, "tableName");
    if (!cJSON_IsString(tableName)) {
        cJSON_Delete(operationJson);
        return NULL;
    }

    Operation operation = malloc(sizeof(struct Operation));
    assert(operation != NULL);

    operation->tableName = strdup(tableName->valuestring);

    if (strcmp(queryType->valuestring, "SELECT") == 0) {
        operation = parseSelectOperation(operation, operationJson);
    } else if (strcmp(queryType->valuestring, "INSERT") == 0) {
        operation = parseInsertOperation(operation, operationJson);
    } else if (strcmp(queryType->valuestring, "UPDATE") == 0) {
        operation = parseUpdateOperation(operation, operationJson);
    } else if (strcmp(queryType->valuestring, "DELETE") == 0) {
        operation = parseDeleteOperation(operation, operationJson);
    } else if (strcmp(queryType->valuestring, "CREATE_TABLE") == 0) {
        operation = parseCreateTableOperation(operation, operationJson);
    } else {
        LOG("Invalid input into parseOperationJson. Did not have queryType "
            "correctly set");
        return NULL;
    }

    cJSON_Delete(operationJson);
    return operation;
}

// QueryResult to JSON

static cJSON *fieldToJson(Field field) {
    cJSON *result = NULL;
    switch (field.type) {
        case INT:
            result = cJSON_CreateNumber(field.intValue);
            break;
        case STR:
            result = cJSON_CreateString(field.stringValue);
            break;
        case VARSTR:
            result = cJSON_CreateString(field.stringValue);
            break;
        case FLOAT:
            result = cJSON_CreateNumber(field.floatValue);
            break;
        case BOOL:
            result = cJSON_CreateBool(field.boolValue);
            break;
        default:;
    }

    assert(result != NULL);
    return result;
}

static void addRecordToObject(cJSON *jsonObject, Record record) {
    for (int i = 0; i < record->numValues; i++) {
        Field field = record->fields[i];
        const char *fieldName = field.attribute;
        cJSON_AddItemToObject(jsonObject, fieldName, fieldToJson(field));
    }
}

char *queryResultStringify(QueryResult queryResult) {
    RecordArray records = queryResult->records;

    cJSON *recordJsonArray = cJSON_CreateArray();
    assert(recordJsonArray != NULL);

    for (int i = 0; i < records->size; i++) {
        cJSON *recordJson = cJSON_CreateObject();
        assert(recordJson != NULL);

        cJSON_AddItemToArray(recordJsonArray, recordJson);

        addRecordToObject(recordJson, records->records[i]);
    }

    char *result = cJSON_Print(recordJsonArray);
    assert(result != NULL);
    return result;
}
