#include "createAndOpenIndex.h"

#include "table/index/b+-tree.h"
#include "test-library.h"

void testCreateAndOpenIndex() {
    createBIndex(4, "code", INT);
    Index index = openIndex("code");

    START_OUTER_TEST("Test creation and opening of index")
    ASSERT_EQ(getD(index), 341)
    ASSERT_EQ(getRootId(index), 1)
    ASSERT_EQ(getKeySize(index), 4)
    FINISH_OUTER_TEST
    PRINT_SUMMARY
    closeIndex(index);
}