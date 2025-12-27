#include "selectThreeArg.h"

#include "table/operations/sqlToOperation.h"
#include "test-library.h"

void testSelectThreeArg() {
    char sql[] = "select name, age, student from students where age between 15 and 24;";

    Operation operation = sqlToOperation(sql);

    START_OUTER_TEST("Test select with range condition")
    ASSERT_EQ(operation->query.select.condition->type, BETWEEN);
    ASSERT_STR_EQ(operation->query.select.condition->value.between.op1->value.strOp, "age");
    ASSERT_EQ(operation->query.select.condition->value.between.op1->type, ATTR);
    ASSERT_EQ(operation->query.select.condition->value.between.op2->value.intOp, 15);
    ASSERT_EQ(operation->query.select.condition->value.between.op2->type, INT);
    ASSERT_EQ(operation->query.select.condition->value.between.op3->value.intOp, 24);
    ASSERT_EQ(operation->query.select.condition->value.between.op3->type, INT);
    FINISH_OUTER_TEST
    PRINT_SUMMARY
}