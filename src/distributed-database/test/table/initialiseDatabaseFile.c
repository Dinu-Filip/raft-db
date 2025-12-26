#include "initialiseDatabaseFile.h"

#include "../../table/core/table.h"
#include "test-library.h"

void testInitialiseDatabaseFile() {
    initialiseTable("testdb");
    TableInfo info = openTable("testdb");

    START_OUTER_TEST("Test empty database file creation");

    ASSERT_STR_EQ(info->name, "testdb");
    ASSERT_EQ(info->header->modified, false);
    ASSERT_EQ(info->header->globalIdx, 0);
    ASSERT_EQ(info->header->numPages, 0);
    ASSERT_EQ(info->header->pageSize, _PAGE_SIZE);

    FINISH_OUTER_TEST
    PRINT_SUMMARY
}