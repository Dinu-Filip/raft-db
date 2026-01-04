#include "insertWithOverflow.h"

#include "table/index/b+-tree.h"
#include "test-library.h"

void testInsertWithOverflow() {
    createBIndex(4, "code", ID_KEY);
    Index index = openIndex("code");

    for (int i = 0; i < 700; i++) {
        addKeyToIndex(index, &i, 30 + i);
    }

    closeIndex(index);
    index = openIndex("code");

    START_OUTER_TEST("Test insertion into the root node with overflow")
    Node root = getNode(index, getRootId(index));
    ASSERT_EQ(root->numKeys, 1)
    ASSERT_EQ(root->type, INTERNAL)
    unsigned leftId = getKeyChild(index, root, 0);
    unsigned rightId = getKeyChild(index, root, 1);
    Node left = getNode(index, leftId);
    Node right = getNode(index, rightId);
    ASSERT_EQ(left->numKeys + right->numKeys, 700);
    ASSERT_EQ(left->type, LEAF);
    ASSERT_EQ(right->type, LEAF);
    FINISH_OUTER_TEST
    PRINT_SUMMARY
}