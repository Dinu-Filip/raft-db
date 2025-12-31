#include "createTable.h"

#include "log.h"
#include "table/core/recordArray.h"
#include "table/core/table.h"
#include "table/operations/createTable.h"
#include "table/operations/select.h"
#include "table/operations/sqlToOperation.h"
#include "test-library.h"

void testCreateTableOperation() {
    char sql[] = "create table students (name varstr(50), age int, height float, student bool, code str(4));";

    createTable(sqlToOperation(sql));
    TableInfo schemaTable = openTable("students-schema");
    Schema schema = getDictSchema();

    char schemaSelect[] = "select * from students-schema";

    QueryResult result = selectOperation(schemaTable, &schema, sqlToOperation(schemaSelect));

    START_OUTER_TEST("Test creation of new table")
    ASSERT_EQ(result->records->size, 5);
    ASSERT_EQ(result->records->records[0]->fields[1].intValue, VARSTR);
    ASSERT_EQ(result->records->records[0]->fields[2].intValue, 50);
    ASSERT_EQ(result->records->records[1]->fields[1].intValue, INT);
    ASSERT_EQ(result->records->records[2]->fields[1].intValue, FLOAT);
    ASSERT_STR_EQ(result->records->records[2]->fields[4].stringValue, "height");
    ASSERT_EQ(result->records->records[3]->fields[1].intValue, BOOL);
    ASSERT_EQ(result->records->records[4]->fields[1].intValue, STR);
    ASSERT_EQ(result->records->records[4]->fields[2].intValue, 4);

    FINISH_OUTER_TEST
    PRINT_SUMMARY
    closeTable(schemaTable);
    LOG("done");
}