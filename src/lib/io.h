#ifndef IO_H
#define IO_H

#include <stdbool.h>
#include <stdio.h>

typedef struct IO *IO;
struct IO {
    FILE *input;
    bool isStdout;
    FILE *output;
};

/**
 * @brief Allocates and initialises on the heap IO struct by opening the
 * required files
 * @param inputFilePath string to the input file's file path
 * @param inputFileMode string to set the mode of operation for the input file
 * @param outputFilePath string to the output file's file path
 * @param outputFileMode string to set the mode of operation for the output file
 * @return pointer to the allocated and initialise io struct
 */
extern IO allocIO(const char *inputFilePath, const char *inputFileMode,
                  const char *outputFilePath, const char *outputFileMode);
/**
 * @brief Closes files and deallocates memory of io struct
 * @param io the pointer to the IO struct to free
 */
extern void freeIO(IO io);

#endif  // IO_H
