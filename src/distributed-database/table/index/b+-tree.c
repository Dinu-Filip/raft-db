#include <assert.h>

#define INIT_ROOT_ID 1

#define ROOT_ID_WIDTH sizeof(uint16_t)
#define D_WIDTH sizeof(unsigned)
#define KEY_SIZE_WIDTH sizeof(unsigned)

#define TABLE_HEADER_WIDTH (ROOT_ID_WIDTH + D_WIDTH + KEY_SIZE_WIDTH)

#include <math.h>

#include "b+-tree.h"

typedef struct Index *Index;
struct Index {
    FILE *file;
    uint16_t rootId;
    unsigned d;
    unsigned keySize;
    int (*cmp)(const void*, const void*);
};

struct Node {
    uint16_t id;
    unsigned numKeys;
    struct Node *prev;
    struct Node *next;
};

static void initialiseBIndex(FILE *index, size_t typeWidth) {
    fseek(index, 0, SEEK_SET);

    uint16_t rootID = INIT_ROOT_ID;
    fwrite(&rootID, sizeof(rootID), 1, index);

    unsigned m = (_PAGE_SIZE - TABLE_HEADER_WIDTH) / (2 * typeWidth);
    unsigned d = ceil(m / 2.0);

    fwrite(&d, D_WIDTH, 1, index);

    unsigned keySize = typeWidth;
    fwrite(&keySize, KEY_SIZE_WIDTH, 1, index);
}

void createBIndex(size_t typeWidth, AttributeName attribute) {
    char indexFile[MAX_FILE_NAME_LEN];
    snprintf(indexFile, sizeof(indexFile), "%s/%s-index.%s", DB_BASE_DIRECTORY, attribute, DB_EXTENSION);

    FILE *index = fopen(indexFile, "wb+");
    assert(index != NULL);

    initialiseBIndex(index, typeWidth);

    fclose(index);
}

static void readIndexHeader(Index index) {
    fseek(index->file, 0, SEEK_SET);

    fread(&index->rootId, ROOT_ID_WIDTH, 1, index->file);
    fread(&index->d, D_WIDTH, 1, index->file);
    fread(&index->keySize, KEY_SIZE_WIDTH, 1, index->file);
}

Index openIndex(AttributeName attribute) {
    char indexFile[MAX_FILE_NAME_LEN];
    snprintf(indexFile, sizeof(indexFile), "%s/%s-index.%s", DB_BASE_DIRECTORY, attribute, DB_EXTENSION);

    FILE *file = fopen(indexFile, "wb+");
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