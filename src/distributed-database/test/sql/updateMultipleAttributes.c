#include "updateMultipleAttributes.h"

#include "table/operations/sqlToOperation.h"
#include "test-library.h"

void testUpdateMultipleAttributes() {
    char sql[] = "update students set name = 'Dinu', age = 20;";

    Operation operation = sqlToOperation(sql);

    START_OUTER_TEST("Test updating multiple attributes with no where")
    ASSERT_EQ(operation->query.update.attributes->numAttributes, 2)
    ASSERT_STR_EQ(operation->query.update.attributes->attributes[0], "name")
    ASSERT_STR_EQ(operation->query.update.attributes->attributes[1], "age")
    ASSERT_EQ(operation->query.update.values->values[0]->type, STR)
    ASSERT_STR_EQ(operation->query.update.values->values[0]->value.strOp, "Dinu")
    ASSERT_EQ(operation->query.update.values->values[1]->type, INT)
    ASSERT_EQ(operation->query.update.values->values[1]->value.intOp, 20)
    FINISH_OUTER_TEST
    PRINT_SUMMARY
}