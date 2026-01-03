#ifndef B_TREE_H
#define B_TREE_H
#include <stddef.h>

#include "table/schema.h"

typedef struct Index *Index;

extern void createBIndex(size_t typeWidth, AttributeName attribute);

extern Index openIndex(AttributeName attribute);

extern void closeIndex(Index index);

#endif //B_TREE_H
