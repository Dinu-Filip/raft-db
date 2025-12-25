#ifndef RECORD_H
#define RECORD_H

#include "table/table.h"

typedef struct Record *Record;
struct Record {
    Field *fields;
    unsigned int numValues;
    size_t size;
    uint32_t globalIdx;
};

typedef struct RecordIterator *RecordIterator;
struct RecordIterator {
    size_t pageId;
    int slotIdx;
    Page page;
    RecordSlot *lastSlot;
};

/**
 * Frees record and fields
 * @param record
 */
extern void freeRecord(Record record);

/**
 * Parses query into internal record representation
 * @param schema schema to parse
 * @param attributes attributes from query
 * @param values values from query
 * @param globalIdx global record index
 * @return Record containing array of fields
 */
extern Record parseQuery(Schema *schema, QueryAttributes attributes,
                         QueryValues values, uint32_t globalIdx);

/**
 * Parses raw bytes in file into Record
 * @param page page to read from
 * @param offset offset to record
 * @param schema schema for parsing
 */
extern Record parseRecord(Page page, size_t offset, Schema *schema);

/**
 * Writes record to page starting backwards from recordEnd
 * @param page page to write record to
 * @param record record to write
 * @param globalIdx global index of record
 * @param recordEnd offset of end of record
 * @return offset to start of record
 */
extern uint16_t writeRecord(Page page, Record record, uint32_t globalIdx,
                            uint16_t recordEnd);

/**
 * Iterates through records in database
 * @param tableInfo table to iterate through
 * @param schema schema for parsing
 * @param recordIterator
 * @param autoClearPage determines whether to free page once exited
 */
extern Record iterateRecords(TableInfo tableInfo, Schema *schema,
                             RecordIterator recordIterator,
                             bool autoClearPage);

/**
 * Frees record iterator
 * @param iterator
 */
extern void freeRecordIterator(RecordIterator iterator);

/**
 * Outputs fields of records
 * @param record
 */
extern void outputRecord(Record record);


#endif //RECORD_H
