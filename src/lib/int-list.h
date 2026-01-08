#ifndef INT_LIST_H
#define INT_LIST_H

#include <stdlib.h>

typedef struct IntList *IntList;

extern IntList createIntList(void);
extern void freeIntList(IntList l);

extern void intListInsert(IntList l, size_t index, int value);
extern void intListPush(IntList l, int value);
extern void intListRemove(IntList l, size_t index);
extern int intListPop(IntList l);
extern void intListSet(IntList l, size_t index, int value);
extern int intListGet(IntList l, size_t index);
extern size_t intListLength(IntList l);
extern void intListForeach(IntList l, void (*f)(int x));

#endif  // INT_LIST_H
