#include "bitmap.h"

#include <assert.h>
#include <stdint.h>
#include <stdlib.h>

typedef uint32_t word_t;

struct bitmap {
    size_t size;
    word_t *bits;
};

bitmap_t initBitmap(size_t size) {
    bitmap_t bitmap = malloc(sizeof(struct bitmap));
    assert(bitmap != NULL);

    word_t *bits = malloc(sizeof(word_t) * (size / sizeof(word_t) + 1));
    assert(bits != NULL);

    bitmap->size = size;
    bitmap->bits = bits;

    return bitmap;
}

void setBit(bitmap_t bitmap, size_t n) {
    word_t *bits = &bitmap->bits[n / sizeof(word_t)];
    *bits |= 1 << (n % sizeof(word_t));
}

void clearBit(bitmap_t bitmap, size_t n) {
    word_t *bits = &bitmap->bits[n / sizeof(word_t)];
    *bits &= ~(1 << (n % sizeof(word_t)));
}

int getBit(bitmap_t bitmap, size_t n) {
    word_t bits = bitmap->bits[n / sizeof(word_t)];
    return bits >> (n % sizeof(word_t)) & 1;
}

void freeBitmap(bitmap_t bitmap) {
    free(bitmap);
}