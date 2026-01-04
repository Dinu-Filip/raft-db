#include <assert.h>

#include "insertToRootUnordered.h"
#include "table/index/b+-tree.h"
#include "test-library.h"

void testInsertToRootUnordered() {
    createBIndex(8, "code", INT_KEY);
    Index index = openIndex("code");

    for (int i = 0; i < 100; i += 2) {
        KeyId id = {.secKey = {.key = &i, .id = 50 + i}};
        addKeyToIndex(index, &id, 30 + i);
    }
    for (int i = 1; i < 100; i += 2) {
        KeyId id = {.secKey = {.key = &i, .id = 50 + i}};
        addKeyToIndex(index, &id, 30 + i);
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
        KeyId id;
        getInternalKey(index, node, i, &id);
        unsigned offset = getKeyChild(index, node, i);
        ASSERT_EQ(*(int *)id.secKey.key, i);
        ASSERT_EQ(id.secKey.id, 50 + i)
        ASSERT_EQ(offset, 30 + i)
    }
    FINISH_OUTER_TEST
    PRINT_SUMMARY
}