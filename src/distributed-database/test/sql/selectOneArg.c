#include "selectOneArg.h"

#include "table/operations/sqlToOperation.h"
#include "test-library.h"

void testSelectOneArg() {
    char sql[] = "select name, age, student from students where not student;";

    Operation operation = sqlToOperation(sql);

    START_OUTER_TEST("Test select with single argument condition")
    ASSERT_STR_EQ(operation->tableName, "students")
    ASSERT_EQ(operation->query.select.attributes->numAttributes, 3)
    ASSERT_EQ(operation->query.select.condition->type, NOT)
    ASSERT_EQ(operation->query.select.condition->value.oneArg.op1->type, ATTR);
    ASSERT_STR_EQ(operation->query.select.condition->value.oneArg.op1->value.strOp, "student")
    FINISH_OUTER_TEST
    PRINT_SUMMARY
}

