#include "insertAllAttributesMultiPage.h"

#include <stdio.h>
#include <unistd.h>

#include "log.h"
#include "networking/msg.h"
#include "table/core/recordArray.h"
#include "table/operations/operation.h"
#include "table/operations/sqlToOperation.h"
#include "test-library.h"

void testInsertAllAttributesMultiPage() {
    char template[] = "insert into students values ('Dinu', %d, %f, %d, 'c924');";

    float start = 1.5;
    for (int i = 0; i < 500; i++) {
        char sql[300];
        snprintf(sql, sizeof(sql), template, i, start + i, i % 2 == 0);
        executeOperation(sqlToOperation(sql));
    }

    char select[] = "select * from students;";
    QueryResult studentRes = executeOperation(sqlToOperation(select));

    char spaceSelect[] = "select * from students-space-inventory;";
    QueryResult spaceRes = executeQualifiedOperation(sqlToOperation(spaceSelect), FREE_MAP);

    START_OUTER_TEST("Test insertion of records across multiple pages")
    ASSERT_EQ(studentRes->records->size, 500)
    for (int i = 0; i < 500; i++) {
        Record record = studentRes->records->records[i];
        ASSERT_STR_EQ(record->fields[0].stringValue, "Dinu")
        ASSERT_EQ(record->fields[1].intValue, i)
        ASSERT_EQ(record->fields[3].boolValue, i % 2 == 0)
    }

    ASSERT_EQ(spaceRes->records->size, 4)
    TableInfo table = openTable("students");
    for (int i = 0; i < spaceRes->records->size; i++) {
        Page page = getPage(table, i + 1);
        ASSERT_EQ(page->header->numRecords, page->header->slots.size)
        ASSERT_EQ(table->header->pageSize, page->header->numRecords * (27 + SLOT_SIZE) + 6 + page->header->freeSpace)
        ASSERT_EQ(spaceRes->records->records[i]->fields[1].intValue, page->header->freeSpace)
        freePage(page);
    }
    closeTable(table);

    FINISH_OUTER_TEST
    PRINT_SUMMARY
}