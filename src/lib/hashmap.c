#include "hashmap.h"

#include <assert.h>
#include <string.h>

#define INITIAL_BUCKETS 32
#define LOAD_FACTOR 0.75

Hashmap createHashmap(void (*freeValue)(Value value)) {
    Hashmap hashmap = malloc(sizeof(struct Hashmap));
    assert(hashmap != NULL);

    hashmap->numBuckets = INITIAL_BUCKETS;
    hashmap->size = 0;
    hashmap->freeValue = freeValue;
    hashmap->buckets = malloc(INITIAL_BUCKETS * sizeof(Node));
    assert(hashmap->buckets != NULL);

    for (int i = 0; i < INITIAL_BUCKETS; i++) {
        hashmap->buckets[i] = NULL;
    }

    return hashmap;
}

void freeHashmap(Hashmap hashmap) {
    for (int i = 0; i < hashmap->numBuckets; i++) {
        Node curr = hashmap->buckets[i];
        while (curr != NULL) {
            Node prev = curr;
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

static size_t hash(Hashmap hashmap, char *key) {
    unsigned int res = 0;
    while (*key != '\0') {
        res = (res + *key) % hashmap->numBuckets;
        key++;
    }
    return res;
}

static void hashmapResize(Hashmap hashmap) {
    if (hashmap->size > LOAD_FACTOR * hashmap->numBuckets) {
        Node *oldBuckets = hashmap->buckets;
        size_t oldNumBuckets = hashmap->numBuckets;

        hashmap->numBuckets *= 2;
        hashmap->size = 0;
        hashmap->buckets = malloc(hashmap->numBuckets * sizeof(Node));

        assert(hashmap->buckets != NULL);

        for (int i = 0; i < hashmap->numBuckets; i++) {
            hashmap->buckets[i] = NULL;
        }

        for (int i = 0; i < oldNumBuckets; i++) {
            Node curr = oldBuckets[i];
            while (curr != NULL) {
                hashmapInsert(hashmap, curr->key, curr->value);
                Node prev = curr;
                curr = curr->next;
                free(prev);
            }
        }
        free(oldBuckets);
    }
}

typedef struct NodePair *NodePair;
struct NodePair {
    Node prev;
    Node curr;
};

static NodePair traverseTo(Hashmap hashmap, char *key) {
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

void hashmapInsert(Hashmap hashmap, char *key, Value value) {
    assert(strlen(key) <= MAX_KEY_SIZE);

    NodePair pair = traverseTo(hashmap, key);
    if (pair == NULL) {
        Node newEntry = malloc(sizeof(struct Node));
        assert(newEntry != NULL);
        strcpy(newEntry->key, key);

        newEntry->value = value;

        size_t bucketIndex = hash(hashmap, key);

        newEntry->next = hashmap->buckets[bucketIndex];

        hashmap->buckets[bucketIndex] = newEntry;

        hashmap->size++;
        hashmapResize(hashmap);
    } else {
        pair->curr->value = value;
        free(pair);
    }
}

Value hashmapGet(Hashmap hashmap, char *key) {
    assert(strlen(key) <= MAX_KEY_SIZE);

    NodePair pair = traverseTo(hashmap, key);
    if (pair == NULL) {
        return NULL;
    }

    Node entry = pair->curr;
    free(pair);

    return entry->value;
}

Value hashmapRemove(Hashmap hashmap, char *key) {
    assert(strlen(key) <= MAX_KEY_SIZE);

    NodePair pair = traverseTo(hashmap, key);

    if (pair == NULL) {
        return NULL;
    }

    pair->prev->next = pair->curr->next;

    Value value = pair->curr->value;

    free(pair->curr);
    free(pair);

    return value;
}
