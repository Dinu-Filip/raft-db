#include <assert.h>

#define INIT_ROOT_ID 1

#define ROOT_ID_WIDTH sizeof(uint16_t)
#define D_WIDTH sizeof(uint16_t)
#define KEY_SIZE_WIDTH sizeof(uint16_t)
#define CMP_TYPE_WIDTH sizeof(uint8_t)

#define NODE_ID_WIDTH sizeof(uint16_t)

#define TABLE_HEADER_WIDTH \
    (ROOT_ID_WIDTH + D_WIDTH + KEY_SIZE_WIDTH + CMP_TYPE_WIDTH)

#include <math.h>

#include "b+-tree.h"

typedef struct Index *Index;
struct Index {
    FILE *file;
    uint16_t rootId;
    uint16_t d;
    uint16_t keySize;
    int (*cmp)(const void *, const void *);
};

struct Node {
    uint16_t id;
    unsigned numKeys;
    struct Node *prev;
    struct Node *next;
};

static int cmpInt(void *x, void *y) { return 0; }

static int cmpStr(void *m, void *n) { return 0; }

static void initialiseBIndex(FILE *index, size_t typeWidth,
                             AttributeType type) {
    fseek(index, 0, SEEK_SET);

    uint16_t rootID = INIT_ROOT_ID;
    fwrite(&rootID, sizeof(rootID), 1, index);

    unsigned m = (_PAGE_SIZE - TABLE_HEADER_WIDTH - NODE_ID_WIDTH) /
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
            index->cmp = cmpInt;
            break;
        case VARSTR:
        case STR:
            index->cmp = cmpStr;
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
    fclose(index->file);
    free(index);
}

unsigned getD(Index index) { return index->d; }

unsigned getKeySize(Index index) { return index->keySize; }

unsigned getRootId(Index index) { return index->rootId; }
