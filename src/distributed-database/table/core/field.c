#include "field.h"

#include <string.h>

#include "log.h"

int getStaticTypeWidth(AttributeType type) {
    switch (type) {
        case INT:
            return INT_WIDTH;
        case FLOAT:
            return FLOAT_WIDTH;
        case BOOL:
            return BOOL_WIDTH;
        default:
            LOG_ERROR("Invalid type\n");
    }
}

void writeField(uint8_t *fieldStart, Field field) {
    void *value;

    switch (field.type) {
        case INT:
            value = &field.intValue;
            break;
        case STR:
        case VARSTR:
            value = field.stringValue;
            break;
        case FLOAT:
            value = &field.floatValue;
            break;
        case BOOL:
            value = &field.boolValue;
            break;
        default:
            LOG_ERROR("Invalid field\n");
    }

    memcpy(fieldStart, value, field.size);
}

void outputField(Field field, unsigned rightPadding) {
    switch (field.type) {
        case INT:
            fprintf(stderr, "%-*d", rightPadding, field.intValue);
            break;
        case FLOAT:
            fprintf(stderr, "%-*f", rightPadding, field.floatValue);
            break;
        case BOOL:
            fprintf(stderr, "%-*d", rightPadding, field.boolValue);
            break;
        case STR:
        case VARSTR:
            fprintf(stderr, "%-*s", rightPadding, field.stringValue);
            break;
        default:
            break;
    }
}

void freeField(Field field) {
    free(field.attribute);

    // String attribute value assumed to have been allocated
    if (field.type == STR || field.type == VARSTR) {
        free(field.stringValue);
    }
}
