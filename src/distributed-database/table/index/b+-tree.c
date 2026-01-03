#include <assert.h>
#include <math.h>
#include <string.h>
#include <unistd.h>

#include "b+-tree.h"

#define INIT_ROOT_ID 0

#define ROOT_ID_WIDTH sizeof(uint16_t)
#define D_WIDTH sizeof(uint16_t)
#define KEY_SIZE_WIDTH sizeof(uint16_t)
#define CMP_TYPE_WIDTH sizeof(uint8_t)

#define NODE_ID_WIDTH sizeof(uint16_t)
#define NODE_PARENT_WIDTH sizeof(uint16_t)
#define NODE_PREV_WIDTH sizeof(uint16_t)
#define NODE_NEXT_WIDTH sizeof(uint16_t)
#define NODE_NUM_KEYS_WIDTH sizeof(uint16_t)
#define NODE_TYPE_WIDTH sizeof(uint16_t)

#define RID_PAGE_WIDTH sizeof(uint16_t)
#define RID_OFFSET_WIDTH sizeof(uint16_t)

#define TABLE_HEADER_WIDTH \
    (ROOT_ID_WIDTH + D_WIDTH + KEY_SIZE_WIDTH + CMP_TYPE_WIDTH)
#define NODE_HEADER_WIDTH \
    (NODE_PREV_WIDTH + NODE_NEXT_WIDTH + NODE_NUM_KEYS_WIDTH + NODE_TYPE_WIDTH)

#define KEY_START NODE_HEADER_WIDTH
#define CHILDREN_START(keySize, d) (KEY_START + keySize * 2 * d)

typedef struct Index *Index;
struct Index {
    FILE *file;
    bool modified;
    uint16_t rootId;
    uint16_t numPages;
    uint16_t d;
    uint16_t keySize;
    int (*cmp)(const void *, const void *);
};

typedef struct InsertArgs InsertArgs;
struct InsertArgs {
    union {
        struct {
            uint16_t leftId;
            uint16_t rightId;
        } children;
        uint16_t offset;
    };
};

static void splitNode(Index index, Node node);

static int intcmp(void *x, void *y) {
    if (*(int *)x < *(int *)y) return -1;
    if (*(int *)x > *(int *)y) return 1;
    return 0;
}

static void initialiseBIndex(FILE *index, size_t typeWidth,
                             AttributeType type) {
    fseek(index, 0, SEEK_SET);

    uint16_t rootID = INIT_ROOT_ID;
    fwrite(&rootID, sizeof(rootID), 1, index);

    unsigned m = (_PAGE_SIZE - NODE_HEADER_WIDTH - NODE_ID_WIDTH) /
                 (typeWidth + NODE_ID_WIDTH);
    unsigned d = ceil(m / 2.0);

    fwrite(&d, D_WIDTH, 1, index);

    unsigned keySize = typeWidth;
    fwrite(&keySize, KEY_SIZE_WIDTH, 1, index);
    fwrite(&type, CMP_TYPE_WIDTH, 1, index);
}

void createBIndex(size_t typeWidth, AttributeName attribute,
                  AttributeType type) {
    char indexFile[MAX_FILE_NAME_LEN];
    snprintf(indexFile, sizeof(indexFile), "%s/%s-index.idx", DB_BASE_DIRECTORY,
             attribute);

    FILE *index = fopen(indexFile, "wb+");
    assert(index != NULL);

    initialiseBIndex(index, typeWidth, type);

    fclose(index);
}

static void readIndexHeader(Index index) {
    fseek(index->file, 0, SEEK_SET);

    fread(&index->rootId, ROOT_ID_WIDTH, 1, index->file);
    fread(&index->d, D_WIDTH, 1, index->file);
    fread(&index->keySize, KEY_SIZE_WIDTH, 1, index->file);

    AttributeType cmpType;
    fread(&cmpType, KEY_SIZE_WIDTH, 1, index->file);

    switch (cmpType) {
        case INT:
            index->cmp = intcmp;
            break;
        case VARSTR:
        case STR:
            index->cmp = strcmp;
            break;
        default:
            exit(-1);
    }
}

Index openIndex(AttributeName attribute) {
    char indexFile[MAX_FILE_NAME_LEN];
    snprintf(indexFile, sizeof(indexFile), "%s/%s-index.idx", DB_BASE_DIRECTORY,
             attribute);

    FILE *file = fopen(indexFile, "rb+");
    assert(file != NULL);

    Index index = malloc(sizeof(struct Index));
    assert(index != NULL);

    index->file = file;

    readIndexHeader(index);

    return index;
}

void closeIndex(Index index) {
    if (index->modified) {
        fseek(index->file, 0, SEEK_SET);
        fwrite(&index->rootId, ROOT_ID_WIDTH, 1, index->file);
    }
    fclose(index->file);
    free(index);
}

unsigned getD(Index index) { return index->d; }

unsigned getKeySize(Index index) { return index->keySize; }

unsigned getRootId(Index index) { return index->rootId; }

unsigned getNumPages(Index index) { return index->numPages; }

void getNodeHeader(Node node) {
    node->headerModified = false;
    node->nodeModified = false;

    memcpy(&node->numKeys, node->ptr, NODE_NUM_KEYS_WIDTH);
    memcpy(&node->type, node->ptr + NODE_NUM_KEYS_WIDTH, NODE_TYPE_WIDTH);
    memcpy(&node->prev, node->ptr + NODE_NUM_KEYS_WIDTH + NODE_TYPE_WIDTH,
           NODE_PREV_WIDTH);
    memcpy(&node->next,
           node->ptr + NODE_NUM_KEYS_WIDTH + NODE_TYPE_WIDTH + NODE_PREV_WIDTH,
           NODE_NEXT_WIDTH);
}

void updateNodeHeader(Node node) {
    node->headerModified = true;

    memcpy(node->ptr, &node->numKeys, NODE_NUM_KEYS_WIDTH);
    memcpy(node->ptr + NODE_NUM_KEYS_WIDTH, &node->type, NODE_TYPE_WIDTH);
    memcpy(node->ptr + NODE_NUM_KEYS_WIDTH + NODE_TYPE_WIDTH, &node->prev,
           NODE_PREV_WIDTH);
    memcpy(node->ptr + NODE_NUM_KEYS_WIDTH + NODE_TYPE_WIDTH + NODE_PREV_WIDTH,
           &node->next, NODE_NEXT_WIDTH);
}

uint16_t getNodeId(Node node) { return node->id; }

Node getNode(Index index, uint16_t id) {
    Node node = malloc(sizeof(struct Node));
    assert(node != NULL);

    fseek(index->file, id * _PAGE_SIZE, SEEK_SET);

    uint8_t *ptr = malloc(sizeof(uint8_t) * _PAGE_SIZE);
    assert(ptr != NULL);

    fread(ptr, _PAGE_SIZE, 1, index->file);

    node->id = id;
    node->ptr = ptr;
    getNodeHeader(node);

    return node;
}

Node addNode(Index index, NodeType type, uint16_t parent, uint16_t prev,
             uint16_t next) {
    Node node = getNode(index, ++index->numPages);
    index->modified = true;
    node->parent = parent;
    node->type = type;
    node->prev = prev;
    node->next = next;
    node->numKeys = 0;

    node->headerModified = true;

    return node;
}

void closeNode(Index index, Node node) {
    fseek(index->file, node->id * _PAGE_SIZE, SEEK_SET);

    if (node->headerModified) {
        updateNodeHeader(node);
        fwrite(node->ptr, sizeof(uint8_t), NODE_HEADER_WIDTH, index->file);
    }
    if (node->nodeModified) {
        fwrite(node->ptr + NODE_HEADER_WIDTH, sizeof(uint8_t),
               _PAGE_SIZE - NODE_HEADER_WIDTH, index->file);
    }

    free(node->ptr);
    free(node);
}

void getInternalKey(Index index, Node node, unsigned idx, void *dest) {
    assert(idx < node->numKeys);

    unsigned size = index->keySize;
    memcpy(dest, node->ptr + KEY_START + size * idx, size);
}

void setInternalKey(Index index, Node node, unsigned idx, void *src) {
    assert(idx < node->numKeys);

    unsigned size = index->keySize;
    memcpy(node->ptr + KEY_START + size * idx, src, size);

    node->nodeModified = true;
}

void setKeyChild(Index index, Node node, unsigned idx, unsigned id) {
    assert(idx < node->numKeys);
    memcpy(node->ptr + CHILDREN_START(index->keySize, index->d) +
               NODE_ID_WIDTH * idx,
           &id, NODE_ID_WIDTH);
    node->nodeModified = true;
}

unsigned getKeyChild(Index index, Node node, unsigned idx) {
    unsigned res;
    assert(idx < node->numKeys);

    memcpy(&res,
           node->ptr + CHILDREN_START(index->keySize, index->d) +
               NODE_ID_WIDTH * idx,
           NODE_ID_WIDTH);
    return res;
}

unsigned searchKey(Index index, Node node, void *key) {
    if (node->numKeys == 0) {
        return 0;
    }

    unsigned left = 0;
    unsigned right = node->numKeys - 1;

    while (left <= right) {
        unsigned mid = (left + right) / 2;

        uint8_t curr[index->keySize];
        getInternalKey(index, node, mid, curr);

        switch (index->cmp(key, curr)) {
            case 0:
                return mid;
            case 1:
                left = mid + 1; break;
            default:
                right = mid - 1; break;
        }
    }

    return left;
}

Node moveToNode(Index index, Node node, void *key) {
    assert(node->type != LEAF);

    unsigned leqKey = searchKey(index, node, key);
    unsigned nextId = getKeyChild(index, node, leqKey + 1);
    closeNode(index, node);
    return getNode(index, nextId);
}

Node traverseTo(Index index, void *key) {
    Node curr = getNode(index, index->rootId);

    while (curr->type != LEAF) {
        curr = moveToNode(index, curr, key);
    }

    return curr;
}

void orderedKeyInsertInternal(Index index, Node node, void *key,
                              unsigned leftId, unsigned rightId) {
    assert(node->numKeys < index->d * 2 - 1);
    unsigned destIdx = searchKey(index, node, key);

    node->numKeys++;
    node->headerModified = true;
    node->nodeModified = true;

    setKeyChild(index, node, node->numKeys,
                getKeyChild(index, node, node->numKeys - 1));

    for (int i = node->numKeys - 1; i > destIdx; i--) {
        uint8_t curr[index->keySize];
        getInternalKey(index, node, i - 1, curr);
        setInternalKey(index, node, i, curr);
        setKeyChild(index, node, i, getKeyChild(index, node, i - 1));
    }

    setInternalKey(index, node, destIdx, key);
    setKeyChild(index, node, destIdx, leftId);
    setKeyChild(index, node, destIdx + 1, rightId);
}

void orderedKeyInsertLeaf(Index index, Node node, void *key, unsigned offset) {
    unsigned destIdx = searchKey(index, node, key);

    node->numKeys++;
    node->headerModified = true;
    node->nodeModified = true;

    for (int i = node->numKeys - 1; i > destIdx; i--) {
        uint8_t curr[index->keySize];
        getInternalKey(index, node, i - 1, curr);
        setInternalKey(index, node, i, curr);
        setKeyChild(index, node, i, getKeyChild(index, node, i - 1));
    }

    setInternalKey(index, node, destIdx, key);
    setKeyChild(index, node, destIdx, offset);
}

void insertKey(Index index, Node node, void *key, InsertArgs args) {
    if (node->numKeys >= index->d * 2 - 1) {
        splitNode(index, node);
    }
    if (node->type == INTERNAL) {
        orderedKeyInsertInternal(index, node, key, args.children.leftId,
                                 args.children.rightId);
    } else {
        orderedKeyInsertLeaf(index, node, key, args.offset);
    }
}

static void splitNode(Index index, Node node) {
    assert(node->numKeys == index->d * 2 - 1);
    Node nextNode =
        addNode(index, node->type, node->parent, node->id, node->next);
    node->next = nextNode->id;

    unsigned d = index->d;
    for (unsigned i = d; i < d * 2; i++) {
        uint8_t curr[index->keySize];
        getInternalKey(index, nextNode, i, curr);
        setInternalKey(index, nextNode, i - d - 1, curr);
        setKeyChild(index, nextNode, i - d - 1,
                    getKeyChild(index, nextNode, i));
        setKeyChild(index, nextNode, i - d,
                    getKeyChild(index, nextNode, i + 1));
    }

    node->numKeys = d;
    nextNode->numKeys = d - 1;
    uint16_t parent = node->parent;

    uint8_t key[index->keySize];
    getInternalKey(index, node, node->numKeys - 1, key);

    if (node->type != LEAF) {
        node->numKeys--;
    }

    closeNode(index, nextNode);

    Node parentNode;
    if (parent != 0) {
        parentNode = getNode(index, parent);
    } else {
        parentNode = addNode(index, INTERNAL, 0, 0, 0);
    }

    InsertArgs args = {
        .children = {.leftId = node->id, .rightId = nextNode->id}};
    insertKey(index, parentNode, key, args);
    closeNode(index, parentNode);
}

void addKeyToIndex(Index index, void *key, unsigned offset) {
    Node leaf;
    if (index->rootId == 0) {
        Node root = addNode(index, LEAF, 0, 0, 0);
        index->rootId = root->id;
        leaf = root;
    } else {
        leaf = traverseTo(index, key);
    }

    InsertArgs args = {.offset = offset};
    insertKey(index, leaf, key, args);
    closeNode(index, leaf);
}