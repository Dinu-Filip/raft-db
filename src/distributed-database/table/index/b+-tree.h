#ifndef B_TREE_H
#define B_TREE_H
#include <stddef.h>

#include "table/schema.h"

typedef struct Index *Index;
typedef struct Node *Node;

typedef enum {
    INTERNAL,
    LEAF
} NodeType;

extern void createBIndex(size_t typeWidth, AttributeName attribute, AttributeType type);

extern Index openIndex(AttributeName attribute);

extern void closeIndex(Index index);

unsigned getD(Index index);
unsigned getKeySize(Index index);
unsigned getRootId(Index index);
unsigned getNumPages(Index index);

uint16_t getNodeId(Node node);
void addKeyToIndex(Index index, void *key, unsigned offset);

#endif //B_TREE_H
