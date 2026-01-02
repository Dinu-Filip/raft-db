#include "updateStaticFields.h"

#include <stdio.h>

#include "table/core/recordArray.h"
#include "table/operations/operation.h"
#include "table/operations/sqlToOperation.h"
#include "test-library.h"

void testUpdateStaticFields() {
    char template[] = "update students set age = %d where student = true";
    char sql[100];
    snprintf(sql, sizeof(sql), template, 20);
    executeOperation(sqlToOperation(sql));
    char select[] = "select * from students where student = true";
    QueryResult res = executeOperation(sqlToOperation(select));
    START_OUTER_TEST("Update static field operation")
    ASSERT_EQ(res->records->size, 250)
    for (int i = 0; i < res->records->size; i++) {
        Record record = res->records->records[i];
        ASSERT_EQ(record->fields[1].intValue, 20)
    }
    FINISH_OUTER_TEST
    PRINT_SUMMARY
}