#include "updateSingleAttribute.h"

#include "log.h"
#include "table/operations/operation.h"
#include "table/operations/sqlToOperation.h"
#include "test-library.h"

void testUpdateSingleAttribute() {
    char sql[] = "update students set name = 'Dinu'";

    Operation operation = sqlToOperation(sql);

    START_OUTER_TEST("Test updating a single attribute with no where clause")
    ASSERT_EQ(operation->queryType, UPDATE)
    ASSERT_STR_EQ(operation->tableName, "students")
    ASSERT_EQ(operation->query.update.attributes->numAttributes, 1)
    ASSERT_STR_EQ(operation->query.update.attributes->attributes[0], "name")
    ASSERT_EQ(operation->query.update.values->numValues, 1)
    ASSERT_EQ(operation->query.update.values->values[0]->type, STR)
    ASSERT_STR_EQ(operation->query.update.values->values[0]->value.strOp, "Dinu")
    FINISH_OUTER_TEST
    PRINT_SUMMARY
}