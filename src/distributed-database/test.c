#include "test.h"

#include <assert.h>

#include "client-handling/input.h"
#include "networking/msg.h"
#include "table/operation.h"
#include "test-library.h"

void assertQueryAttributesEquals(QueryAttributes a, QueryAttributes b) {
    START_OUTER_TEST("QueryAtributes EQ");
    ASSERT_LIST_STR_EQ(a->attributes, a->numAttributes, b->attributes,
                       b->numAttributes);
    FINISH_OUTER_TEST
}

void assertOperandEquals(Operand a, Operand b) {
    START_OUTER_TEST("Operand EQ");
    ASSERT_EQ(a->type, b->type);
    switch (a->type) {
        case INT:
            ASSERT_EQ(a->value.intOp, b->value.intOp);
            break;
        case STR:
        case VARSTR:
            ASSERT_STR_EQ(a->value.strOp, b->value.strOp);
            break;
        case FLOAT:
            ASSERT_EQ(a->value.floatOp, b->value.floatOp);
            break;
        case BOOL:
            ASSERT_EQ(a->value.boolOp, b->value.boolOp);
            break;
    }
    FINISH_OUTER_TEST
}

void assertQueryValueEquals(QueryValues a, QueryValues b) {
    START_OUTER_TEST("QueryValues EQ");
    ASSERT_EQ(a->numValues, b->numValues);
    for (int i = 0; i < a->numValues; i++) {
        assertOperandEquals(a->values[i], b->values[i]);
    }
    FINISH_OUTER_TEST
}

void assertConditionEquals(Condition a, Condition b) {
    START_OUTER_TEST("Condition EQ");
    ASSERT_EQ(a->type, b->type);
    switch (a->type) {
        case EQUALS:
        case LESS_THAN:
        case GREATER_THAN:
        case AND:
        case OR:
            ASSERT_STR_EQ(a->value.twoArg.op1, b->value.twoArg.op1)
            assertOperandEquals(a->value.twoArg.op2, b->value.twoArg.op2);
            break;
        case BETWEEN:
            ASSERT_STR_EQ(a->value.between.op1, b->value.between.op1);
            assertOperandEquals(a->value.between.op2, b->value.between.op2);
            assertOperandEquals(b->value.between.op3, b->value.between.op3);
            break;
        case NOT:
            ASSERT_STR_EQ(a->value.oneArg.op1, b->value.oneArg.op1);
            break;
    }
    FINISH_OUTER_TEST
}

void assertQueryTypesEquals(QueryTypes a, QueryTypes b) {
    START_OUTER_TEST("QueryTypes EQ");
    ASSERT_LIST_EQ(a->types, a->numTypes, b->types, b->numTypes);
    ASSERT_LIST_EQ(a->sizes, a->numSizes, b->sizes, b->numSizes);
    FINISH_OUTER_TEST
}

void assertOperationEquals(Operation a, Operation b) {
    START_OUTER_TEST("Operation EQ");
    ASSERT_STR_EQ(a->tableName, b->tableName);
    ASSERT_EQ(a->queryType, b->queryType);
    switch (a->queryType) {
        case SELECT:
            assertQueryAttributesEquals(a->query.select.attributes,
                                        b->query.select.attributes);
            assertConditionEquals(a->query.select.condition,
                                  b->query.select.condition);
            break;
        case INSERT:
            assertQueryAttributesEquals(a->query.insert.attributes,
                                        b->query.insert.attributes);
            assertQueryValueEquals(a->query.insert.values,
                                   b->query.insert.values);
            break;
        case UPDATE:
            assertQueryAttributesEquals(a->query.update.attributes,
                                        b->query.update.attributes);
            assertQueryValueEquals(a->query.update.values,
                                   b->query.update.values);
            assertConditionEquals(a->query.update.condition,
                                  b->query.update.condition);
            break;
        case DELETE:
            assertConditionEquals(a->query.delete.condition,
                                  b->query.delete.condition);
            break;
        case CREATE_TABLE:
            assertQueryAttributesEquals(a->query.createTable.attributes,
                                        b->query.createTable.attributes);
            assertQueryTypesEquals(a->query.createTable.types,
                                   b->query.createTable.types);
            break;
    }
    FINISH_OUTER_TEST
}

static QueryAttributes makeQueryAttributes(const char *attrs[], int numAttrs) {
    QueryAttributes res = malloc(sizeof(struct QueryAttributes));
    res->numAttributes = numAttrs;
    res->attributes = malloc(numAttrs * sizeof(AttributeName));
    memcpy(res->attributes, attrs, numAttrs * sizeof(AttributeName));
    return res;
}

static Operand makeIntOperand(int val) {
    Operand res = malloc(sizeof(struct Operand));
    res->type = INT;
    res->value.intOp = val;
    return res;
}

static Operand makeStringOperand(char *string) {
    Operand res = malloc(sizeof(struct Operand));
    res->type = STR;
    res->value.strOp = string;
    return res;
}

static Operand makeFloatOperand(float val) {
    Operand res = malloc(sizeof(struct Operand));
    res->type = FLOAT;
    res->value.floatOp = val;
    return res;
}

static Operand makeBoolOperand(bool val) {
    Operand res = malloc(sizeof(struct Operand));
    res->type = BOOL;
    res->value.boolOp = val;
    return res;
}

static QueryValues makeQueryValues(const Operand *ops, int numOps) {
    QueryValues res = malloc(sizeof(struct QueryValues));
    res->numValues = numOps;
    res->values = malloc(numOps * sizeof(Operand));
    memcpy(res->values, ops, numOps * sizeof(Operand));
    return res;
}

static Condition makeOneArgCondition(QueryType type, AttributeName attr) {
    Condition res = malloc(sizeof(struct Condition));
    res->type = type;
    res->value.oneArg.op1 = attr;
    return res;
}

static void freeSelectOperation(Operation operation) {
    free(operation->query.select.attributes->attributes);
    free(operation->query.select.attributes);
    free(operation->query.select.condition);
    free(operation);
}

static void freeInsertOperation(Operation operation) {
    free(operation->query.insert.attributes->attributes);
    free(operation->query.insert.attributes);
    free(operation->query.insert.values->values);
    free(operation->query.insert.values);
    free(operation);
}

static void testSelect1() {
    START_OUTER_TEST("Test Select 1")
    const char *json =
        "{"
        "\"tableName\": \"firstTable\","
        "\"queryType\": \"SELECT\","
        "\"attributes\": ["
        "\"a\","
        "\"b\","
        "\"c\""
        "],"
        "\"condition\": {\"type\": \"EQUALS\", \"op1\":\"a\", \"op2\": 2}"
        "}";
    Operation output = parseOperationJson(json);
    Operation expected = malloc(sizeof(struct Operation));
    assert(expected != NULL);
    expected->tableName = "firstTable";
    expected->queryType = SELECT;
    const char *attrs[] = {"a", "b", "c"};
    expected->query.select.attributes = makeQueryAttributes(attrs, 3);
    expected->query.select.condition = makeOneArgCondition(EQUALS, "a");
    expected->query.select.condition->value.twoArg.op2 = makeIntOperand(2);
    assertOperationEquals(output, expected);
    freeSelectOperation(output);
    freeSelectOperation(expected);
    FINISH_OUTER_TEST
}

static void testSelect2() {
    START_OUTER_TEST("Test Select 2")
    const char *json =
        "{"
        "\"tableName\": \"secondTable\","
        "\"queryType\": \"SELECT\","
        "\"attributes\": ["
        "\"a\","
        "\"b\""
        "],"
        "\"condition\": {\"type\": \"NOT\", \"op1\":\"a\"}"
        "}";
    Operation output = parseOperationJson(json);
    Operation expected = malloc(sizeof(struct Operation));
    assert(expected != NULL);
    expected->tableName = "secondTable";
    expected->queryType = SELECT;
    const char *attrs[] = {"a", "b"};
    expected->query.select.attributes = makeQueryAttributes(attrs, 2);
    expected->query.select.condition = makeOneArgCondition(NOT, "a");
    assertOperationEquals(output, expected);
    freeSelectOperation(output);
    freeSelectOperation(expected);
    FINISH_OUTER_TEST
}

static void testInsert1() {
    START_OUTER_TEST("Test Insert 1")
    const char *json =
        "{"
        "\"tableName\": \"firstTable\","
        "\"queryType\": \"INSERT\","
        "\"attributes\": ["
        "\"a\","
        "\"b\","
        "\"c\""
        "],"
        "\"values\": [1, 2, 3]"
        "}";
    Operation output = parseOperationJson(json);
    Operation expected = malloc(sizeof(struct Operation));
    assert(expected != NULL);
    expected->tableName = "firstTable";
    expected->queryType = INSERT;
    const char *attrs[] = {"a", "b", "c"};
    expected->query.insert.attributes = makeQueryAttributes(attrs, 3);
    const Operand values[] = {makeIntOperand(1), makeIntOperand(2),
                              makeIntOperand(3)};
    expected->query.insert.values = makeQueryValues(values, 3);
    assertOperationEquals(output, expected);
    freeInsertOperation(output);
    freeInsertOperation(expected);
    FINISH_OUTER_TEST
}

static void testInsert2() {
    START_OUTER_TEST("Test Insert 1")
    const char *json =
        "{"
        "\"tableName\": \"secondTable\","
        "\"queryType\": \"INSERT\","
        "\"attributes\": ["
        "\"a\","
        "\"b\","
        "\"c\","
        "\"longer-name\""
        "],"
        "\"values\": [\"test\", 2.1, 3, true]"
        "}";
    Operation output = parseOperationJson(json);
    Operation expected = malloc(sizeof(struct Operation));
    assert(expected != NULL);
    expected->tableName = "secondTable";
    expected->queryType = INSERT;
    const char *attrs[] = {"a", "b", "c", "longer-name"};
    expected->query.insert.attributes = makeQueryAttributes(attrs, 4);
    const Operand values[] = {makeStringOperand("test"), makeFloatOperand(2.1),
                              makeIntOperand(3), makeBoolOperand(true)};
    expected->query.insert.values = makeQueryValues(values, 4);
    assertOperationEquals(output, expected);
    freeInsertOperation(output);
    freeInsertOperation(expected);
    FINISH_OUTER_TEST
}

int main(void) {
    testSelect1();
    testSelect2();
    testInsert1();
    testInsert2();
    PRINT_SUMMARY
    return 0;
}
