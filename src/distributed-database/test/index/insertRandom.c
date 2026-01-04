#include "insertRandom.h"

#include "table/index/b+-tree.h"

void testInsertRandom() {
    createBIndex(4, "code", INT_KEY);
    Index index = openIndex("code");

    for (int i = 0; i < 700; i++) {
        KeyId key = {.secKey = {.key = &i, .id = 30 + i}};
        addKeyToIndex(index, &key, 30 + i);
    }
}