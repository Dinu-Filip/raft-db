#include "selectAll.h"

#include "table/core/field.h"
#include "table/core/recordArray.h"
#include "table/operations/select.h"
#include "table/operations/sqlToOperation.h"
#include "table/schema.h"
#include "test-library.h"
#include "test/table/multiplePageDummy.h"

#define CREATE_ATTR(schema, idx, name_, type_, size_, loc_) ({\
schema->attrInfos[idx].name = name_; \
schema->attrInfos[idx].type = type_;\
schema->attrInfos[idx].size = size_;\
schema->attrInfos[idx].loc = loc_;\
})

void testSelectAllRecords() {
    createMultiplePageDummy();

    Schema schema;

    schema.numAttrs = 7;
    schema.attrInfos = malloc(sizeof(AttrInfo) * schema.numAttrs);

    Schema *s = &schema;
    CREATE_ATTR(s, 0, "id", INT, INT_WIDTH, 0);
    CREATE_ATTR(s, 1, "age", INT, INT_WIDTH, INT_WIDTH);
    CREATE_ATTR(s, 2, "height", FLOAT, FLOAT_WIDTH, INT_WIDTH * 2);
    CREATE_ATTR(s, 3, "student", BOOL, BOOL_WIDTH, INT_WIDTH * 2 + FLOAT_WIDTH);
    CREATE_ATTR(s, 4, "num", INT, INT_WIDTH, INT_WIDTH * 2 + FLOAT_WIDTH + BOOL_WIDTH);
    CREATE_ATTR(s, 5, "email", VARSTR, 50, 0);
    CREATE_ATTR(s, 6, "name", VARSTR, 50, 1);

    TableInfo table = openTable("testdb");

    char sql[] = "select * from testdb";
    QueryResult result = selectOperation(table, &schema, sqlToOperation(sql));

    START_OUTER_TEST("Test select with all attributes and no condition")
    ASSERT_EQ(result->records->size, 500)
    ASSERT_EQ(result->records->records[100]->numValues, 7)
    ASSERT_STR_EQ(result->records->records[100]->fields[6].stringValue, "Dinu")
    FINISH_OUTER_TEST
    PRINT_SUMMARY
}