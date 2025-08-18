#include "io.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "log.h"

#define READ_64_BITS 1

IO allocIO(const char *inputFilePath, const char *inputFileMode,
           const char *outputFilePath, const char *outputFileMode) {
    const IO io = malloc(sizeof(struct IO));

    if (io == NULL) {
        LOG_ERROR("malloc of io failed");
    }

    LOG("Opening input file %s in mode %s", inputFilePath, inputFileMode);
    io->input = fopen(inputFilePath, inputFileMode);
    if (io->input == NULL) {
        LOG_PERROR("Failed to open input file");
    }

    io->isStdout = strcmp(outputFilePath, "") == 0;

    if (!io->isStdout) {
        LOG("Opening output file %s in mode %s", outputFilePath,
            outputFileMode);
    }
    io->output = io->isStdout ? stdout : fopen(outputFilePath, outputFileMode);
    if (io->output == NULL) {
        LOG_PERROR("Failed to open output file");
    }

    return io;
}

void freeIO(IO io) {
    int error = fclose(io->input);
    if (error != 0) {
        perror("Failed to close input file");
    }

    if (!io->isStdout) {
        error = fclose(io->output);
        if (error != 0) {
            perror("Failed to close output file");
        }
    }

    free(io);
}
