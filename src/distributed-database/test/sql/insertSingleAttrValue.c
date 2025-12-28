#include "insertSingleAttrValue.h"

#include "table/operations/sqlToOperation.h"
#include "test-library.h"

void testInsertSingleAttrValue() {
    char sql[] = "insert into students (name) values ('Dinu');";

    Operation operation = sqlToOperation(sql);

    START_OUTER_TEST("Test insertion of a record with a single attribute")
    ASSERT_EQ(operation->queryType, INSERT);
    ASSERT_STR_EQ(operation->tableName, "students");
    ASSERT_EQ(operation->query.insert.attributes->numAttributes, 1);
    ASSERT_STR_EQ(operation->query.insert.attributes->attributes[0], "name");
    ASSERT_EQ(operation->query.insert.values->numValues, 1);
    ASSERT_EQ(operation->query.insert.values->values[0]->type, STR);
    ASSERT_STR_EQ(operation->query.insert.values->values[0]->value.strOp, "Dinu");
    FINISH_OUTER_TEST
    PRINT_SUMMARY
}