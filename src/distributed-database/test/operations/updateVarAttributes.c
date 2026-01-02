#include "updateVarAttributes.h"

#include <assert.h>
#include <stdbool.h>
#include <stdio.h>

#include "log.h"
#include "table/core/recordArray.h"
#include "table/operations/operation.h"
#include "table/operations/sqlToOperation.h"
#include "test-library.h"

void testUpdateVarAttributes() {
    char template[] = "update students set name = '%s' where student = %d";
    char sql[100];

    snprintf(sql, sizeof(sql), template, "D", true);
    executeOperation(sqlToOperation(sql));
    snprintf(sql, sizeof(sql), template, "Constantin", false);
    executeOperation(sqlToOperation(sql));

    char select1[] = "select name from students where student = true;";
    QueryResult res1 = executeOperation(sqlToOperation(select1));
    char select2[] = "select name from students where student = false;";
    QueryResult res2 = executeOperation(sqlToOperation(select2));

    START_OUTER_TEST("Test updating variable length fields")
    ASSERT_EQ(res1->records->size + res2->records->size, 500);
    TableInfo table = openTable("students");
    for (int i = 0; i < 5; i++) {
        Page page = getPage(table, i + 1);
        LOG("%d", page->header->numRecords);
    }
    for (int i = 0; i < res1->records->size; i++) {
        Record record = res1->records->records[i];
        ASSERT_STR_EQ(record->fields[0].stringValue, "D")
    }
    for (int i = 0; i < res2->records->size; i++) {
        Record record = res2->records->records[i];
        ASSERT_STR_EQ(record->fields[0].stringValue, "Constantin")
    }
    FINISH_OUTER_TEST
    PRINT_SUMMARY
}