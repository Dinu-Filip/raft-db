#include <assert.h>

#include "insertToRootUnordered.h"
#include "table/index/b+-tree.h"
#include "test-library.h"

void testInsertToRootUnordered() {
    createBIndex(4, "code", INT);
    Index index = openIndex("code");

    for (int i = 0; i < 100; i += 2) {
        addKeyToIndex(index, &i, 30 + i);
    }
    for (int i = 1; i < 100; i += 2) {
        addKeyToIndex(index, &i, 30 + i);
    }

    closeIndex(index);
    index = openIndex("code");


    START_OUTER_TEST("Test insertion in order into the root")
    assert(index != 0);
    Node node = getNode(index, getRootId(index));
    ASSERT_EQ(node->numKeys, 100)
    ASSERT_EQ(node->type, LEAF)
    ASSERT_EQ(node->parent, 0)
    ASSERT_EQ(node->keyType, INT)
    for (int i = 0; i < 100; i++) {
        int32_t x;
        getInternalKey(index, node, i, &x);
        unsigned offset = getKeyChild(index, node, i);
        ASSERT_EQ(x, i);
        ASSERT_EQ(offset, 30 + i)
    }
    FINISH_OUTER_TEST
    PRINT_SUMMARY
}