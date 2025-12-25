#ifndef RECORDARRAY_H
#define RECORDARRAY_H

#include "record.h"

typedef struct RecordArray *RecordArray;
struct RecordArray {
    Record *records;
    int size;
    int capacity;
};

/**
 * Creates resizing record array
 */
extern RecordArray createRecordArray();

/**
 * Frees resizing record array
 * @param records
 */
extern void freeRecordArray(RecordArray records);

/**
 * Adds record to resizing record array
 * @param records array of records
 * @param record record to add
 */
extern void addRecord(RecordArray records, Record record);

#endif //RECORDARRAY_H
