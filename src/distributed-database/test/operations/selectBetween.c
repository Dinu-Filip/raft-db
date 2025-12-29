#include "selectBetween.h"

#include "table/core/field.h"
#include "table/core/recordArray.h"
#include "table/core/table.h"
#include "table/operations/select.h"
#include "table/operations/sqlToOperation.h"
#include "table/schema.h"
#include "test-library.h"
#include "test/table/multiplePageDummy.h"

void testSelectBetween() {
    createMultiplePageDummy();

    Schema schema;
    AttributeName names[7] = {"id", "age", "height", "student", "num", "email", "name"};
    AttributeType types[7] = {INT, INT, FLOAT, BOOL, INT, VARSTR, VARSTR};
    unsigned sizes[7] = {INT_WIDTH, INT_WIDTH, FLOAT_WIDTH, BOOL_WIDTH, INT_WIDTH, 50, 50};

    schema.attributes = names;
    schema.attributeTypes = types;
    schema.attributeSizes = sizes;
    schema.numAttributes = 7;

    TableInfo table = openTable("testdb");

    char sql[] = "select id, email, height from testdb where id between 134 and 233;";
    QueryResult result = selectOperation(table, &schema, sqlToOperation(sql));

    START_OUTER_TEST("Test selecting from within a range")
    ASSERT_EQ(result->records->size, 100)
    ASSERT_EQ(result->records->records[0]->numValues, 3)
    ASSERT_EQ(result->records->records[99]->numValues, 3)
    ASSERT_EQ(result->records->records[0]->fields[0].intValue, 134)
    ASSERT_EQ(result->records->records[99]->fields[0].intValue, 233)
    FINISH_OUTER_TEST
    PRINT_SUMMARY
}