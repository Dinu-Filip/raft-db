#ifndef BUFFER_H
#define BUFFER_H
#include <stddef.h>
#include <stdint.h>

typedef struct Frame Frame;
typedef struct Buffer *Buffer;

Buffer initialiseBuffer();
Frame *allocFilePage(Buffer buffer, int fd, size_t pageId);
void pgcpy(Frame *frame, uint16_t offset, void *dest, size_t size);
void pgset(Frame *frame, uint16_t offset, void *src, size_t size);
void freeFrame(Buffer buffer, Frame *frame);

#endif //BUFFER_H
