#ifndef INT_HASHMAP_H
#define INT_HASHMAP_H

typedef struct IntHashmap *IntHashmap;

extern IntHashmap createIntHashmap(void (*freeValue)(void *value));
extern void freeIntHashmap(IntHashmap hashmap);
extern void addIntHashmap(IntHashmap hashmap, int key, void *value);
extern void *getIntHashmap(IntHashmap hashmap, int key);

#endif
