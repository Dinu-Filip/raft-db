#include <stdio.h>

#include "insertAllAttributesSinglePage.h"
#include "log.h"
#include "table/core/recordArray.h"
#include "table/operations/operation.h"
#include "table/operations/sqlToOperation.h"
#include "test-library.h"

void testInsertAllAttributesSinglePage() {
    char template[] = "insert into students values ('Dinu', %d, %f, %d, 'c924');";

    float start = 1.5;
    for (int i = 0; i < 50; i++) {
        char sql[100];
        snprintf(sql, sizeof(sql), template, i, start + i, i % 2 == 0);
        executeOperation(sqlToOperation(sql));
    }

    char select[] = "select * from students;";
    QueryResult res = executeOperation(sqlToOperation(select));
    START_OUTER_TEST("Test insertion of records over a single page")
    ASSERT_EQ(res->records->size, 50)
    for (int i = 0; i < 49; i++) {
        Record record = res->records->records[i];
        ASSERT_EQ(record->fields[1].intValue, i)
        ASSERT_STR_EQ(record->fields[4].stringValue, "c924")
    }
    FINISH_OUTER_TEST
    PRINT_SUMMARY
}