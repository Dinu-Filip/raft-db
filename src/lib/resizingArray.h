#ifndef RESIZING_ARRAY_H
#define RESIZING_ARRAY_H

#include <assert.h>
#include <stdlib.h>

#define INITIAL_CAPACITY 16

#define DECLARE_RESIZING_ARRAY(type)                                         \
                                                                             \
    typedef struct type##_array type##_array_t;                              \
    struct type##_array {                                                    \
        type *array;                                                         \
        size_t size;                                                         \
        size_t actual_size;                                                  \
        size_t capacity;                                                     \
    };                                                                       \
                                                                             \
    static type##_array_t *create_##type##_array() {                         \
        type *array = (type *)calloc(INITIAL_CAPACITY, sizeof(type));        \
        assert(array != NULL);                                               \
        type##_array_t *resizingArray =                                      \
            (type##_array_t *)malloc(sizeof(type##_array_t));                \
        assert(resizingArray != NULL);                                       \
                                                                             \
        resizingArray->array = array;                                        \
        resizingArray->size = 0;                                             \
        resizingArray->actual_size = 0;                                      \
        resizingArray->capacity = INITIAL_CAPACITY;                          \
        return resizingArray;                                                \
    }                                                                        \
                                                                             \
    static void free_##type##_array(type##_array_t *array) {                 \
        for (int i = 0; i < array->actual_size; i++) {                       \
            free(array->array[i]);                                           \
        }                                                                    \
        free(array->array);                                                  \
        free(array);                                                         \
    }                                                                        \
                                                                             \
    static void type##_array_resize(type##_array_t *array) {                 \
        if (array->size == array->capacity) {                                \
            type *newArray =                                                 \
                (type *)realloc(array->array, array->capacity * 2);          \
            array->array = newArray;                                         \
            array->capacity *= 2;                                            \
        }                                                                    \
    }                                                                        \
                                                                             \
    static void type##_array_insert(type##_array_t *array, size_t idx,       \
                                    type value) {                            \
        assert(0 <= idx && idx <= array->size);                              \
                                                                             \
        for (int i = array->actual_size - 1; i >= array->size; i--) {        \
            free(array->array[i]);                                           \
        }                                                                    \
        for (int i = array->size; i > idx; i--) {                            \
            array->array[i] = array->array[i - 1];                           \
        }                                                                    \
        array->array[idx] = value;                                           \
        array->size += 1;                                                    \
        array->actual_size = array->size;                                    \
        type##_array_resize(array);                                          \
    }                                                                        \
                                                                             \
    static inline void type##_array_add(type##_array_t *array, type value) { \
        type##_array_insert(array, array->size, value);                      \
    }                                                                        \
                                                                             \
    static void type##_array_pop(type##_array_t *array, size_t idx) {        \
        assert(0 <= idx && idx < array->size);                               \
        type prevValue = array->array[idx];                                  \
                                                                             \
        for (int i = idx; i < array->size - 1; i++) {                        \
            array->array[i] = array->array[i + 1];                           \
        }                                                                    \
                                                                             \
        array->size--;                                                       \
        array->actual_size--;                                                \
        free(prevValue);                                                     \
    }                                                                        \
                                                                             \
    static void type##_clear(type##_array_t *array) { array->size = 0; }     \
                                                                             \
    static type type##_array_get(type##_array_t *array, size_t idx) {        \
        return array->array[idx];                                            \
    }

#endif  // RESIZING_ARRAY_H
