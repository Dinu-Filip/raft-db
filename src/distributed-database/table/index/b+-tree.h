#ifndef B_TREE_H
#define B_TREE_H
#include <stddef.h>

#include "table/schema.h"

typedef struct Index *Index;

extern void createBIndex(size_t typeWidth, AttributeName attribute, AttributeType type);

extern Index openIndex(AttributeName attribute);

extern void closeIndex(Index index);

unsigned getD(Index index);
unsigned getKeySize(Index index);
unsigned getRootId(Index index);

#endif //B_TREE_H
