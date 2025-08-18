#ifndef FUNCTION_HASHMAP_H
#define FUNCTION_HASHMAP_H

#include <stdlib.h>

#define MAX_KEY_SIZE 30

typedef void (*GenericFunction)();

typedef struct FunctionNode *FunctionNode;
struct FunctionNode {
    char key[MAX_KEY_SIZE + 1];
    GenericFunction value;
    FunctionNode next;
};

typedef struct FunctionHashmap *FunctionHashmap;
struct FunctionHashmap {
    size_t numBuckets;
    size_t size;
    FunctionNode *buckets;
    void (*freeValue)(GenericFunction value);
};

FunctionHashmap createFunctionHashmap(void (*freeValue)(GenericFunction value));
void freeFunctionHashmap(FunctionHashmap hashmap);
void functionHashmapInsert(FunctionHashmap hashmap, char *key,
                           GenericFunction value);
GenericFunction functionHashmapGet(FunctionHashmap hashmap, char *key);
GenericFunction functionHashmapRemove(FunctionHashmap hashmap, char *key);

#endif  // FUNCTION_HASHMAP_H
