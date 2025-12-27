#include "selectTwoArg.h"

#include "table/operations/operation.h"
#include "table/operations/sqlToOperation.h"
#include "test-library.h"

void testSelectTwoArg() {
    char sql[] = "select name, age, student from students where name = 'Dinu'";

    Operation operation = sqlToOperation(sql);
    START_OUTER_TEST("Test select with equality condition")
    ASSERT_EQ(operation->query.select.condition->type, EQUALS);
    ASSERT_STR_EQ(operation->query.select.condition->value.twoArg.op1->value.strOp, "name");
    ASSERT_EQ(operation->query.select.condition->value.twoArg.op1->type, ATTR);
    ASSERT_STR_EQ(operation->query.select.condition->value.twoArg.op2->value.strOp, "Dinu");
    ASSERT_EQ(operation->query.select.condition->value.twoArg.op2->type, STR);
    FINISH_OUTER_TEST
    PRINT_SUMMARY
}
