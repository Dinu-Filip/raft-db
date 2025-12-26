#ifndef LOG_H
#define LOG_H

#include <stdio.h>
#include <stdlib.h>
#define DEBUG

#ifdef DEBUG
#define LOG(fmt, ...) fprintf(stderr, fmt "\n", ##__VA_ARGS__)
#else
#define LOG(fmt, ...) ((void)0)
#endif

#define LOG_ERROR(fmt, ...)                   \
    fprintf(stderr, fmt "\n", ##__VA_ARGS__); \
    exit(EXIT_FAILURE);

#define LOG_PERROR(msg) \
    perror(msg);        \
    exit(EXIT_FAILURE);

#endif  // LOG_H
