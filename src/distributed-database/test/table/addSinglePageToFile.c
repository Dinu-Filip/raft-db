#include "../../table/core/pages.h"
#include "../../table/core/table.h"
#include "addSinglePageToFile.h"
#include "test-library.h"

void testAddSinglePageToFile() {
    TableInfo info = openTable("testdb");
    unsigned numPages = info->header->numPages;

    Page page = addPage(info);
    updatePage(info, page);

    closeTable(info);

    TableInfo newInfo = openTable("testdb");

    START_OUTER_TEST("Test adding a new page to a database file")
    printf("num pages: %d\n", numPages);
    ASSERT_EQ(numPages + 1, newInfo->header->numPages);

    Page newPage = getPage(info, newInfo->header->numPages);
    ASSERT_EQ(newPage->pageId, newInfo->header->numPages);
    ASSERT_EQ(newPage->header->recordStart, _PAGE_SIZE);
    ASSERT_EQ(newPage->header->modified, false);
    ASSERT_EQ(newPage->header->numRecords, 0);
    ASSERT_EQ(newPage->header->freeSpace, _PAGE_SIZE - 3 * NUM_SLOTS_WIDTH);

    FINISH_OUTER_TEST
    PRINT_SUMMARY
    closeTable(newInfo);
}