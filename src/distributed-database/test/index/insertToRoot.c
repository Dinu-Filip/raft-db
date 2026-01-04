#include "insertToRoot.h"

#include <assert.h>

#include "table/index/b+-tree.h"
#include "test-library.h"

void testInsertToRoot() {
    createBIndex(4 + GLOBAL_ID_WIDTH, "code", INT_KEY);
    Index index = openIndex("code");

    for (int i = 0; i < 100; i++) {
        KeyId key = {.secKey = {.key = &i, .id = 30 + i}};
        addKeyToIndex(index, &key, 50 + i);
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
        KeyId keyId;
        getInternalKey(index, node, i, &keyId);
        unsigned offset = getKeyChild(index, node, i);
        ASSERT_EQ(*(int *)keyId.secKey.key, i);
        ASSERT_EQ(keyId.secKey.id, 30 + i);
        ASSERT_EQ(offset, 50 + i)
    }
    FINISH_OUTER_TEST
    PRINT_SUMMARY
}