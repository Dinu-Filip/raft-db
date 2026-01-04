#ifndef B_TREE_H
#define B_TREE_H

#include "table/schema.h"

typedef struct Index *Index;

typedef enum {
    INT_KEY,
    STR_KEY,
    ID_KEY,
} KeyType;

typedef struct KeyId KeyId;
struct KeyId {
    union {
        uint32_t priKey;
        struct {
            void *key;
            uint32_t id;
        } secKey;
    };
};

typedef struct StrKeyId StrKeyId;

typedef enum { INTERNAL, LEAF } NodeType;

typedef struct Node *Node;
struct Node {
    uint8_t *ptr;
    bool headerModified;
    bool nodeModified;
    uint16_t id;
    AttributeType keyType;
    NodeType type;
    uint16_t parent;
    uint16_t numKeys;
    uint16_t prev;
    uint16_t next;
    uint16_t leafDirectoryId;
};

extern void createBIndex(size_t typeWidth, AttributeName attribute,
                         KeyType type);

extern Index openIndex(AttributeName attribute);

extern void closeIndex(Index index);

unsigned getD(Index index);
unsigned getKeySize(Index index);
unsigned getRootId(Index index);
unsigned getNumPages(Index index);

Node getNode(Index index, uint16_t id);
void closeNode(Index index, Node node);

uint16_t getNodeId(Node node);
void getInternalKey(Index index, Node node, unsigned idx, KeyId *dest);
unsigned getKeyChild(Index index, Node node, unsigned idx);
void addKeyToIndex(Index index, KeyId *key, unsigned offset);

#endif  // B_TREE_H
