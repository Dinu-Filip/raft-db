#ifndef HASHMAP_H
#define HASHMAP_H

#include <stdlib.h>

#define MAX_KEY_SIZE 30

typedef void *Value;

typedef struct Node *Node;
struct Node {
    char key[MAX_KEY_SIZE + 1];
    Value value;
    Node next;
};

typedef struct Hashmap *Hashmap;
struct Hashmap {
    size_t numBuckets;
    size_t size;
    Node *buckets;
    void (*freeValue)(Value value);
};

Hashmap createHashmap(void (*freeValue)(Value value));
void freeHashmap(Hashmap hashmap);
void hashmapInsert(Hashmap hashmap, char *key, Value value);
Value hashmapGet(Hashmap hashmap, char *key);
Value hashmapRemove(Hashmap hashmap, char *key);

#endif  // HASHMAP_H
