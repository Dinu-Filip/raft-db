#include "insertRecordsSinglePage.h"

#include "singlePageDummy.h"
#include "table/core/field.h"
#include "table/core/pages.h"
#include "table/core/table.h"
#include "test-library.h"

void testInsertRecordsSinglePage() {
    createSinglePageDummy();

    unsigned recordSize = RECORD_HEADER_WIDTH + SLOT_SIZE * 2 + GLOBAL_ID_WIDTH + INT_WIDTH + INT_WIDTH + FLOAT_WIDTH + BOOL_WIDTH + INT_WIDTH + 25 + 4;
    TableInfo newInfo = openTable("testdb");

    START_OUTER_TEST("Test insertion of and iteration over records in a single page")
    ASSERT_EQ(newInfo->header->numPages, 1);
    Page first = getPage(newInfo, 1);
    ASSERT_EQ(first->header->slots.size, 50);
    ASSERT_EQ(first->header->recordStart, _PAGE_SIZE - 50 * recordSize);
    ASSERT_EQ(first->header->freeSpace, _PAGE_SIZE - 50 * recordSize - 50 * SLOT_SIZE - 3 * NUM_SLOTS_WIDTH);
    ASSERT_EQ(first->header->modified, false);
    ASSERT_EQ(first->header->numRecords, 50);
    FINISH_OUTER_TEST
    PRINT_SUMMARY

    closeTable(newInfo);
}