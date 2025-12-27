#include "selectMultipleAttributes.h"

#include "table/operations/sqlToOperation.h"
#include "test-library.h"

void testSelectMultipleAttributes() {
    char sql[] = "select name, age, id from students;";

    Operation operation = sqlToOperation(sql);

    START_OUTER_TEST("Test select with multiple attribute and no where clause")
    ASSERT_STR_EQ(operation->tableName, "students");
    ASSERT_EQ(operation->queryType, SELECT);
    ASSERT_EQ(operation->query.select.attributes->numAttributes, 3);
    ASSERT_STR_EQ(operation->query.select.attributes->attributes[0], "name");
    ASSERT_STR_EQ(operation->query.select.attributes->attributes[1], "age");
    ASSERT_STR_EQ(operation->query.select.attributes->attributes[2], "id");
    FINISH_OUTER_TEST
    PRINT_SUMMARY

}