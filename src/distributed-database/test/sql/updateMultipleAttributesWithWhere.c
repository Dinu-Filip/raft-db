#include "updateMultipleAttributesWithWhere.h"

#include <math.h>

#include "log.h"
#include "table/operations/sqlToOperation.h"
#include "test-library.h"

void testUpdateMultipleAttributesWithWhere() {
    char sql[] = "update students set name = 'Dinu', age = 20 where height > 194.6;";

    Operation operation = sqlToOperation(sql);

    START_OUTER_TEST("Test updating multiple attributes with where clause")
    ASSERT_EQ(operation->query.update.attributes->numAttributes, 2);
    ASSERT_EQ(operation->query.update.condition->type, GREATER_THAN);
    ASSERT_EQ(operation->query.update.condition->value.twoArg.op1->type, ATTR);
    ASSERT_STR_EQ(operation->query.update.condition->value.twoArg.op1->value.strOp, "height");
    ASSERT_EQ(operation->query.update.condition->value.twoArg.op2->type, FLOAT);

    if (fabsf(operation->query.update.condition->value.twoArg.op2->value.floatOp - 194.6) > 1e-4) {
        FAIL
    }

    FINISH_OUTER_TEST
    PRINT_SUMMARY

}