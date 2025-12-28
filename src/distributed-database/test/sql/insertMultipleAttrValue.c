#include "insertMultipleAttrValue.h"

#include "table/operations/operation.h"
#include "table/operations/sqlToOperation.h"
#include "test-library.h"

void testInsertMultipleAttrValue() {
    char sql[] = "insert into students (name, age, height, student) values ('Dinu', 20, 194.5, true);";

    Operation operation = sqlToOperation(sql);
    START_OUTER_TEST("Test insertion of record with multiple attributes and values")
    ASSERT_STR_EQ(operation->tableName, "students")
    ASSERT_EQ(operation->queryType, INSERT)
    ASSERT_EQ(operation->query.insert.attributes->numAttributes, 4)
    ASSERT_STR_EQ(operation->query.insert.attributes->attributes[0], "name")
    ASSERT_STR_EQ(operation->query.insert.attributes->attributes[1], "age")
    ASSERT_STR_EQ(operation->query.insert.attributes->attributes[2], "height")
    ASSERT_STR_EQ(operation->query.insert.attributes->attributes[3], "student")
    ASSERT_EQ(operation->query.insert.values->numValues, 4)
    ASSERT_EQ(operation->query.insert.values->values[0]->type, STR)
    ASSERT_EQ(operation->query.insert.values->values[1]->type, INT)
    ASSERT_EQ(operation->query.insert.values->values[2]->type, FLOAT)
    ASSERT_EQ(operation->query.insert.values->values[3]->type, BOOL)
    ASSERT_STR_EQ(operation->query.insert.values->values[0]->value.strOp, "Dinu")
    ASSERT_EQ(operation->query.insert.values->values[1]->value.intOp, 20)
    ASSERT_EQ(operation->query.insert.values->values[2]->value.floatOp, 194.5)
    ASSERT_EQ(operation->query.insert.values->values[3]->value.boolOp, true);
    FINISH_OUTER_TEST
    PRINT_SUMMARY
}