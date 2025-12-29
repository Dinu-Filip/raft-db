#include "selectAttrSubset.h"

#include "table/core/field.h"
#include "table/core/recordArray.h"
#include "table/core/table.h"
#include "table/operations/select.h"
#include "table/operations/sqlToOperation.h"
#include "table/schema.h"
#include "test-library.h"
#include "test/table/multiplePageDummy.h"

void testSelectAttrSubset() {
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

    char sql[] = "select email, height from testdb;";
    QueryResult result = selectOperation(table, &schema, sqlToOperation(sql));

    START_OUTER_TEST("Test selecting a subset of attributes in different order")
    ASSERT_EQ(result->records->size, 500);
    ASSERT_EQ(result->records->records[100]->numValues, 2);
    ASSERT_EQ(result->records->records[100]->fields[0].type, VARSTR);
    ASSERT_EQ(result->records->records[100]->fields[1].type, FLOAT);
    ASSERT_STR_EQ(result->records->records[100]->fields[0].attribute, "email")
    ASSERT_STR_EQ(result->records->records[100]->fields[1].attribute, "height")
    FINISH_OUTER_TEST
    PRINT_SUMMARY
}