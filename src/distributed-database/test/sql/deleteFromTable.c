#include "deleteFromTable.h"

#include "table/operations/sqlToOperation.h"
#include "test-library.h"

void testDeleteFromTable() {
    char sql[] = "delete from student where name = 'Dinu';";

    Operation operation = sqlToOperation(sql);

    START_OUTER_TEST("Test delete statement parsing")
    ASSERT_EQ(operation->queryType, DELETE);
    ASSERT_STR_EQ(operation->tableName, "student");
    ASSERT_EQ(operation->query.delete.condition->type, EQUALS);
    ASSERT_EQ(operation->query.delete.condition->value.twoArg.op1->type, ATTR);
    ASSERT_STR_EQ(operation->query.delete.condition->value.twoArg.op1->value.strOp, "name");
    ASSERT_EQ(operation->query.delete.condition->value.twoArg.op2->type, STR);
    ASSERT_STR_EQ(operation->query.delete.condition->value.twoArg.op2->value.strOp, "Dinu");
    FINISH_OUTER_TEST
    PRINT_SUMMARY
}