#include "select.h"

#include <assert.h>
#include <stdlib.h>
#include <string.h>

#include "../conditions.h"
#include "../core/record.h"
#include "../core/recordArray.h"
#include "../core/table.h"
#include "log.h"

static void formatRecord(Record record, QueryAttributes attributes) {
    // Filters out fields that are not specified in Operation

    size_t newSize = 0;
    size_t numAttributes = attributes->numAttributes;
    Field fields[numAttributes];

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

    // Reallocates fields array to hold fields corresponding to given attributes
    record->fields = realloc(record->fields, sizeof(Field) * numAttributes);
    assert(record->fields != NULL);

    memcpy(record->fields, fields, sizeof(Field) * numAttributes);

    record->numValues = numAttributes;
    record->size = newSize;
}

QueryResult selectFrom(TableInfo tableInfo, Schema *schema, Condition cond,
                       QueryAttributes attributes) {
    QueryResult result = malloc(sizeof(struct QueryResult));
    assert(result != NULL);

    // Initialises array to hold records
    RecordArray recordArray = createRecordArray();
    assert(recordArray != NULL);

    result->records = recordArray;

    struct RecordIterator iterator;
    initialiseRecordIterator(&iterator);

    Record record = iterateRecords(tableInfo, schema, &iterator, true);

    while (record != NULL) {
        // Selects record if there is either no condition or the condition
        // evaluates to true
        if (cond == NULL || evaluate(record, cond)) {
            formatRecord(record, attributes);
            addRecord(recordArray, record);
        } else {
            // Frees records that are not selected
            freeRecord(record);
        }

        record = iterateRecords(tableInfo, schema, &iterator, true);
    }

    freeRecordIterator(&iterator);
    return result;
}

QueryResult selectOperation(TableInfo tableInfo, Schema *schema,
                            Operation operation) {
    return selectFrom(tableInfo, schema, operation->query.select.condition,
                      operation->query.select.attributes);
}
