#include "selectOneAttribute.h"

#include "../../table/operations/sqlToOperation.h"
#include "test-library.h"

void testSelectOneAttribute() {
    char sql[] = "select name from students;";

    Operation operation = sqlToOperation(sql);

    START_OUTER_TEST("Test SELECT parse with single attribute and table, and no WHERE")
    ASSERT_EQ(operation->queryType, SELECT);
    ASSERT_STR_EQ(operation->tableName, "students");
    ASSERT_EQ(operation->query.select.attributes->numAttributes, 1);
    ASSERT_STR_EQ(operation->query.select.attributes->attributes[0], "name");

    FINISH_OUTER_TEST
    PRINT_SUMMARY
}