#include "int-list.h"

#include <assert.h>
#include <stdlib.h>
#include <string.h>

#include "utils.h"

#define INITIAL_CAPACITY 16

typedef struct IntList *IntList;
struct IntList {
    int *data;
    size_t length;
    size_t capacity;
};

IntList createIntList(void) {
    IntList l = malloc(sizeof(struct IntList));
    if (l == NULL) {
        return NULL;
    }

    l->length = 0;
    l->capacity = INITIAL_CAPACITY;
    l->data = malloc(sizeof(int) * l->capacity);
    if (l->data == NULL) {
        free(l);
        return NULL;
    }

    return l;
}

void freeIntList(IntList l) {
    free(l->data);
    free(l);
}

static void resize(IntList l, int newCapacityMin) {
    if (l->length >= l->capacity || newCapacityMin >= l->capacity) {
        l->capacity = MAX(l->capacity * 2, newCapacityMin);
        l->data = realloc(l->data, sizeof(int) * l->capacity);
        assert(l->data != NULL);
    }
}

void intListInsert(IntList l, size_t index, int value) {
    assert(0 <= index && index <= l->length);

    resize(l, 1);

    memmove(&l->data[index] + 1, &l->data[index],
            sizeof(int) * (l->length - index));

    l->data[index] = value;
    l->length++;
}

void intListPush(IntList l, int value) { intListInsert(l, l->length, value); }

void intListRemove(IntList l, size_t index) {
    assert(0 <= index && index < l->length);

    memmove(&l->data[index], &l->data[index + 1],
            sizeof(int) * (l->length - index - 1));

    l->length--;
}

int intListPop(IntList l) {
    int last = intListGet(l, l->length - 1);
    intListRemove(l, l->length - 1);
    return last;
}

void intListSet(IntList l, size_t index, int value) {
    resize(l, index);

    l->data[index] = value;
}

int intListGet(IntList l, size_t index) {
    assert(0 <= index && index < l->length);

    return l->data[index];
}

size_t intListLength(IntList l) { return l->length; }

void intListForeach(IntList l, void (*f)(int x)) {
    for (int i = 0; i < l->length; i++) {
        f(l->data[i]);
    }
}
