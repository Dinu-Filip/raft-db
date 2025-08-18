#ifndef UTILS_H
#define UTILS_H

#include <stdint.h>

#define MAX(x, y) (x > y ? x : y)
#define MIN(x, y) (x > y ? y : x)

#define MSB_INDEX_IN_64_BIT_NUM 63
#define MSB_INDEX_IN_32_BIT_NUM 31

// Gets the bit range moves to lowest bit with limit and offset like in SQL
#define GET_BIT_RANGE(x, n, o) ((x >> n) & ((1 << o) - 1))
#define GET_BIT(x, n) GET_BIT_RANGE(x, n, 1)

// Checks if the bits enabled in the mask match the bits in pattern
#define MATCHES_MASK(x, m, p) ((x & m) == p)

#define ALL_ONES (~((uint64_t)0))

// The bits enabled in m are set to each of their respective values in p
#define BITS_MATCH(x, m, p) ((x & m) == p)

typedef enum { BYTE = 1, HALF_WORD = 2, WORD = 4, DOUBLE_WORD = 8 } DataSize;

#define MASK(l) ((1U << l) - 1)

/**
 * @brief Get the sign bit from a signed integer
 * @param x the value to get the sign bit from
 * @param sf the width to read `x` as, if 1 then 64 bits and if 0 then 32 bits
 * @return The sign bit of `x`
 */
extern uint64_t getSign(uint64_t x, unsigned int sf);

/**
 * @brief Sign extend a signed integer
 * @param value the value to sign extend
 * @param size the number of bits to sign extend to
 * @return `value` sign extended to `size` bits
 */
extern uint64_t signExtend(uint64_t value, unsigned int size);

#endif  // UTILS_H
