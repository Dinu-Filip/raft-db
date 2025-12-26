#ifndef FIELD_H
#define FIELD_H
#include "../operations/operation.h"

#define INT_WIDTH 4
#define STR_WIDTH 255
#define FLOAT_WIDTH 4
#define BOOL_WIDTH 1

typedef struct AttributeInfo AttributeInfo;
struct AttributeInfo {
    char *name;
    AttributeType type;
    int size;
    bool primary;
};

typedef struct Field Field;
struct Field {
    char *attribute;
    AttributeType type;
    unsigned size;
    union {
        int32_t intValue;
        uint32_t uintValue;
        float floatValue;
        bool boolValue;
        char *stringValue;
    };
};

extern int getStaticTypeWidth(AttributeType type);

/**
 * Writes field to page
 * @param fieldStart pointer to start of field
 * @param field field to write
 */
extern void writeField(uint8_t *fieldStart, Field field);

extern void outputField(Field field, unsigned int rightPadding);

/**
 * Frees attribute and value of field (for VARSTR and STR)
 * @param field
 */
extern void freeField(Field field);

#endif //FIELD_H
