#include <assert.h>
#include <limits.h>
#include <stdbool.h>
#include <stdint.h>
#include <string.h>

#include "../conditions.h"
#include "../core/pages.h"
#include "../core/record.h"
#include "../core/table.h"
#include "insert.h"
#include "log.h"
#include "operation.h"
#include "table/core/recordArray.h"

static void updateField(Field *field, Operand op) {
    switch (field->type) {
        case INT:
            field->intValue = op->value.intOp;
            break;
        case STR:
            field->stringValue = strdup(op->value.strOp);
            break;
        case VARSTR:
            field->stringValue = strdup(op->value.strOp);
            size_t newLen = strlen(op->value.strOp);
            assert(newLen <= INT_MAX);
            field->size = newLen;
            break;
        case FLOAT:
            field->floatValue = op->value.floatOp;
            break;
        case BOOL:
            field->boolValue = op->value.boolOp;
            break;
        default:
            break;
    }
}

static void updateRecord(TableInfo tableInfo,
                         TableInfo spaceMap, Record record, Page page,
                         QueryAttributes queryAttributes,
                         QueryValues queryValues, RecordIterator iterator, Schema *schema) {
    LOG("Space record");
    outputRecord(record);
    size_t oldSize = record->size;
    for (int j = 0; j < record->numValues; j++) {
        // Only updates fields specified in attributes list
        for (int i = 0; i < queryAttributes->numAttributes; i++) {
            AttributeName attribute = queryAttributes->attributes[i];
            Field *field = &record->fields[j];

            // Skips if attribute and field do not match
            if (strcmp(field->attribute, attribute) != 0) {
                continue;
            }

            // Size of field before update
            unsigned oldFieldSize = field->size;

            // Contents of string field were dynamically allocated
            if (field->type == VARSTR || field->type == STR) {
                free(field->stringValue);
            }

            // Updates field with new value
            updateField(field, queryValues->values[i]);

            // Only variable-length string can change size of record
            if (field->type == VARSTR) {
                record->size += field->size - oldFieldSize;
            }
        }
    }

    if (record->size > oldSize) {
        // Record cannot fit in old position so needs to be removed
        removeRecord(page, iterator->lastSlot, record->size);

        // Defragments page to remove extra space
        defragmentRecords(page);
        updatePage(tableInfo, page);

        // Updates space inventory for page
        if (spaceMap != NULL) {
            updateSpaceInventory(spaceMap->name, spaceMap, page);
        }

        // Insertion will find new page in which to allocate record
        insertRecord(tableInfo, spaceMap, record, RELATION);
        return;
    }

    uint8_t *recordPtr = page->ptr + iterator->lastSlot->offset;
    writeRecord(recordPtr, record);

    // Record takes up less space than before so page needs to be defragmented
    if (record->size < oldSize) {
        LOG("Update record in place\n");
        defragmentRecords(iterator->page);
        if (spaceMap != NULL) {
            updateSpaceInventory(spaceMap->name, spaceMap, page);
        }
    }
    updatePage(tableInfo, page);
    if (spaceMap == NULL) {
        QueryResult spaceMapRes = getFreeSpaces(tableInfo, 27);
        if (spaceMapRes->records->size > 0) {
            LOG("After update");
            outputRecord(spaceMapRes->records->records[0]);
        }
    }
}

void updateTable(TableInfo tableInfo, TableInfo spaceMap,
                 QueryAttributes queryAttributes, QueryValues queryValues,
                 Condition cond, Schema *schema) {
    struct RecordIterator iterator;
    initialiseRecordIterator(&iterator);

    // Iterates through records, updating those that satisfy condition
    bool canContinue = iterateRecords(tableInfo, &iterator, true);

    while (canContinue) {
        Record record = parseRecord(iterator.page->ptr + iterator.lastSlot->offset, schema);
        // Updates record that satisfies condition
        if (evaluate(record, cond)) {
            updateRecord(tableInfo, spaceMap, record, iterator.page,
                         queryAttributes, queryValues, &iterator, schema);
        }

        freeRecord(record);
        canContinue = iterateRecords(tableInfo, &iterator, true);
    }

    freeRecordIterator(&iterator);
}

void updateOperation(TableInfo table, TableInfo spaceMap, Schema *schema,
                     Operation operation) {
    updateTable(table, spaceMap, operation->query.update.attributes,
                operation->query.update.values,
                operation->query.update.condition, schema);
}
