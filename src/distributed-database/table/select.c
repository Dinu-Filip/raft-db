#include "select.h"

#include <assert.h>
#include <stdlib.h>
#include <string.h>

#include "conditions.h"
#include "core/record.h"
#include "log.h"
#include "table.h"

static void filterRecord(Record record, QueryAttributes attributes) {
    // Filters out fields that are not specified in Operation

    size_t newSize = 0;
    size_t newNumFields = attributes->numAttributes;
    Field fields[record->numValues];

    for (int j = 0; j < record->numValues; j++) {
        bool added = false;
        for (int i = 0; i < attributes->numAttributes; i++) {
            AttributeName attribute = attributes->attributes[i];

            // Finds corresponding field in record and places in correct
            // position
            if (strcmp(record->fields[j].attribute, attribute) == 0) {
                fields[i] = record->fields[j];
                newSize += fields[i].size;
                added = true;
                break;
            }
        }

        if (!added) {
            freeField(record->fields[j]);
        }
    }

    memcpy(record->fields, fields, sizeof(Field) * newNumFields);

    record->numValues = newNumFields;
    record->size = newSize;
}

QueryResult selectFrom(TableInfo tableInfo, Schema *schema, Condition cond,
                       QueryAttributes attributes) {
    QueryResult result = malloc(sizeof(struct QueryResult));
    assert(result != NULL);

    result->numRecords = 0;

    RecordArray recordArray = createRecordArray();
    assert(recordArray != NULL);

    result->records = recordArray;

    RecordIterator iterator;
    initialiseRecordIterator(&iterator);

    Record record = iterateRecords(tableInfo, schema, &iterator, true);

    while (record != NULL) {
        if (evaluate(record, cond)) {
            filterRecord(record, attributes);
            addRecord(recordArray, record);
            result->numRecords++;
        } else {
            // Frees records that are not returned
            freeRecord(record);
        }
        record = iterateRecords(tableInfo, schema, &iterator, true);
    }

    freeRecordIterator(iterator);
    return result;
}

QueryResult selectOperation(TableInfo tableInfo, Schema *schema,
                            Operation operation) {
    return selectFrom(tableInfo, schema, operation->query.select.condition,
                      operation->query.select.attributes);
}
