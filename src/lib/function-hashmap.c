#include "function-hashmap.h"

#include <assert.h>
#include <string.h>

#define INITIAL_BUCKETS 32
#define LOAD_FACTOR 0.75

FunctionHashmap createFunctionHashmap(
    void (*freeValue)(GenericFunction value)) {
    FunctionHashmap hashmap = malloc(sizeof(struct FunctionHashmap));
    assert(hashmap != NULL);

    hashmap->numBuckets = INITIAL_BUCKETS;
    hashmap->size = 0;
    hashmap->freeValue = freeValue;
    hashmap->buckets = malloc(INITIAL_BUCKETS * sizeof(FunctionNode));
    assert(hashmap->buckets != NULL);

    for (int i = 0; i < INITIAL_BUCKETS; i++) {
        hashmap->buckets[i] = NULL;
    }

    return hashmap;
}

void freeFunctionHashmap(FunctionHashmap hashmap) {
    for (int i = 0; i < hashmap->numBuckets; i++) {
        FunctionNode curr = hashmap->buckets[i];
        while (curr != NULL) {
            FunctionNode prev = curr;
            curr = curr->next;
            if (hashmap->freeValue != NULL) {
                hashmap->freeValue(prev->value);
            }
            free(prev);
        }
    }
    free(hashmap->buckets);
    free(hashmap);
}

static size_t hash(FunctionHashmap hashmap, char *key) {
    unsigned int res = 0;
    while (*key != '\0') {
        res = (res + *key) % hashmap->numBuckets;
        key++;
    }
    return res;
}

static void functionHashmapResize(FunctionHashmap hashmap) {
    if (hashmap->size > LOAD_FACTOR * hashmap->numBuckets) {
        FunctionNode *oldBuckets = hashmap->buckets;
        size_t oldNumBuckets = hashmap->numBuckets;

        hashmap->numBuckets *= 2;
        hashmap->size = 0;
        hashmap->buckets = malloc(hashmap->numBuckets * sizeof(FunctionNode));

        assert(hashmap->buckets != NULL);

        for (int i = 0; i < hashmap->numBuckets; i++) {
            hashmap->buckets[i] = NULL;
        }

        for (int i = 0; i < oldNumBuckets; i++) {
            FunctionNode curr = oldBuckets[i];
            while (curr != NULL) {
                functionHashmapInsert(hashmap, curr->key, curr->value);
                FunctionNode prev = curr;
                curr = curr->next;
                free(prev);
            }
        }
        free(oldBuckets);
    }
}

typedef struct NodePair *NodePair;
struct NodePair {
    FunctionNode prev;
    FunctionNode curr;
};

static NodePair traverseTo(FunctionHashmap hashmap, char *key) {
    assert(strlen(key) <= MAX_KEY_SIZE);
    size_t bucketIdx = hash(hashmap, key);

    NodePair pair = malloc(sizeof(struct NodePair));
    assert(pair != NULL);
    pair->curr = hashmap->buckets[bucketIdx];
    pair->prev = NULL;

    while (pair->curr != NULL) {
        if (strcmp(pair->curr->key, key) == 0) {
            return pair;
        }
        pair->prev = pair->curr;
        pair->curr = pair->curr->next;
    }

    free(pair);

    return NULL;
}

void functionHashmapInsert(FunctionHashmap hashmap, char *key,
                           GenericFunction value) {
    assert(strlen(key) <= MAX_KEY_SIZE);

    NodePair pair = traverseTo(hashmap, key);
    if (pair == NULL) {
        FunctionNode newEntry = malloc(sizeof(struct FunctionNode));
        assert(newEntry != NULL);
        strcpy(newEntry->key, key);

        newEntry->value = value;

        size_t bucketIndex = hash(hashmap, key);

        newEntry->next = hashmap->buckets[bucketIndex];

        hashmap->buckets[bucketIndex] = newEntry;

        hashmap->size++;
        functionHashmapResize(hashmap);
    } else {
        pair->curr->value = value;
        free(pair);
    }
}

GenericFunction functionHashmapGet(FunctionHashmap hashmap, char *key) {
    assert(strlen(key) <= MAX_KEY_SIZE);

    NodePair pair = traverseTo(hashmap, key);
    if (pair == NULL) {
        return NULL;
    }

    FunctionNode entry = pair->curr;
    free(pair);

    return entry->value;
}

GenericFunction functionHashmapRemove(FunctionHashmap hashmap, char *key) {
    assert(strlen(key) <= MAX_KEY_SIZE);

    NodePair pair = traverseTo(hashmap, key);

    if (pair == NULL) {
        return NULL;
    }

    pair->prev->next = pair->curr->next;

    GenericFunction value = pair->curr->value;

    free(pair->curr);
    free(pair);

    return value;
}
