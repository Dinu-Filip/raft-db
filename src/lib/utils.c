#include "utils.h"

#include <time.h>

uint64_t monotonicNs(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000000000ULL + (uint64_t)ts.tv_nsec;
}

uint64_t getSign(uint64_t x, unsigned int sf) {
    return sf == 1 ? GET_BIT(x, MSB_INDEX_IN_64_BIT_NUM)
                   : GET_BIT(x, MSB_INDEX_IN_32_BIT_NUM);
}

uint64_t signExtend(uint64_t value, unsigned int size) {
    const uint64_t m = 1U << (size - 1);
    return (value ^ m) - m;
}
