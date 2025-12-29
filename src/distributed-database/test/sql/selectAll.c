#include "selectAll.h"

#include "table/operations/sqlToOperation.h"
#include "test-library.h"

void testSelectAll() {
    char sql[] = "select * from students;";

    Operation operation = sqlToOperation(sql);
    START_OUTER_TEST("Test select with *")
    ASSERT_STR_EQ(operation->tableName, "students")
    ASSERT_EQ(operation->query.select.attributes->numAttributes, 0);
    ASSERT_EQ(operation->query.select.condition, NULL);
    FINISH_OUTER_TEST
    PRINT_SUMMARY
}