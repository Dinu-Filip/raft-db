#include "insertNoAttributes.h"

#include "table/operations/sqlToOperation.h"
#include "test-library.h"

void testInsertNoAttributes() {
    char sql[] = "insert into students values ('Dinu', 20, 194.5, true);";

    Operation operation = sqlToOperation(sql);

    START_OUTER_TEST("Test insert parsing with no attributes names")
    ASSERT_EQ(operation->query.insert.attributes->numAttributes, 0);
    ASSERT_EQ(operation->query.insert.values->numValues, 4);
    ASSERT_STR_EQ(operation->query.insert.values->values[0]->value.strOp, "Dinu")
    ASSERT_EQ(operation->query.insert.values->values[1]->value.intOp, 20)
    ASSERT_EQ(operation->query.insert.values->values[2]->value.floatOp, 194.5)
    ASSERT_EQ(operation->query.insert.values->values[3]->value.boolOp, true);
    FINISH_OUTER_TEST
    PRINT_SUMMARY
}