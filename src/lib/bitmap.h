#ifndef BITMAP_H
#define BITMAP_H
#include <stddef.h>

typedef struct bitmap *bitmap_t;

bitmap_t initBitmap(size_t size);
void setBit(bitmap_t bitmap, size_t n);
void clearBit(bitmap_t bitmap, size_t n);
int getBit(bitmap_t bitmap, size_t n);
void freeBitmap(bitmap_t bitmap);

#endif //BITMAP_H
