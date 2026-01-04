#include "insertWithOverflow.h"

#include "table/index/b+-tree.h"
#include "test-library.h"

void testInsertWithOverflow() {
    createBIndex(8, "code", INT_KEY);
    Index index = openIndex("code");

    for (int i = 0; i < 700; i++) {
        KeyId id = {.secKey = {.key = &i, .id = 50 + i}};
        addKeyToIndex(index, &id, 30 + i);
    }

    closeIndex(index);
    index = openIndex("code");

    START_OUTER_TEST("Test insertion into the root node with overflow")
    Node root = getNode(index, getRootId(index));
    ASSERT_EQ(root->numKeys, 2)
    ASSERT_EQ(root->type, INTERNAL)
    unsigned leafKeys = 0;
    for (int i = 1; i <= getNumPages(index); i++) {
        if (i == root->id) {
            continue;
        }
        Node node = getNode(index, i);
        leafKeys += node->numKeys;
        ASSERT_EQ(node->type, LEAF)
        closeNode(index, node);
    }
    ASSERT_EQ(leafKeys, 700)
    FINISH_OUTER_TEST
    PRINT_SUMMARY
}