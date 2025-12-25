#include "recordArray.h"

#include <assert.h>
#define INITIAL_RECORD_CAPACITY 1

RecordArray createRecordArray() {
    RecordArray recordArray = malloc(sizeof(struct RecordArray));
    assert(recordArray != NULL);

    Record *records = malloc(sizeof(Record) * INITIAL_RECORD_CAPACITY);
    assert(records != NULL);

    recordArray->records = records;
    recordArray->capacity = INITIAL_RECORD_CAPACITY;
    recordArray->size = 0;

    return recordArray;
}

void freeRecordArray(RecordArray records) {
    for (int i = 0; i < records->size; i++) {
        Record record = records->records[i];
        freeRecord(record);
    }
    free(records->records);
    free(records);
}

void resizeRecordArray(RecordArray records) {
    if (records->capacity == records->size) {
        records->capacity *= 2;
        records->records =
            realloc(records->records, sizeof(Record) * records->capacity);
        assert(records->records != NULL);
    }
}

void addRecord(RecordArray records, Record record) {
    records->records[records->size++] = record;
    resizeRecordArray(records);
}
