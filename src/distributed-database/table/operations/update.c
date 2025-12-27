#include <assert.h>
#include <limits.h>
#include <stdbool.h>
#include <stdint.h>
#include <string.h>

#include "../conditions.h"
#include "../core/pages.h"
#include "../core/record.h"
#include "../core/table.h"
#include "../db-utils.h"
#include "delete.h"
#include "insert.h"
#include "log.h"
#include "operation.h"
#include "select.h"

static void updatePageHeaderUpdate(Page page, Record record, uint16_t oldSize,
                                   RecordSlot *slot, uint16_t recordStart) {
    LOG("Update page header of page %d\n", page->pageId);
    // Updates free space log in page header
    page->header->freeSpace -= oldSize - record->size;
    page->header->modified = true;

    assert(record->size <= INT16_MAX);
    slot->size = record->size;
    slot->offset = recordStart;

    slot->modified = true;
}

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

static void updateRecord(TableInfo tableInfo, Schema *schema,
                         TableInfo spaceMap, Record record, Page page,
                         QueryAttributes queryAttributes,
                         QueryValues queryValues, RecordIterator iterator) {
    size_t oldSize = record->size;
    for (int j = 0; j < record->numValues; j++) {
        // Only updates fields specified in attributes list
        for (int i = 0; i < queryAttributes->numAttributes; i++) {
            AttributeName attribute = queryAttributes->attributes[i];
            Field *field = &record->fields[j];
            if (strcmp(field->attribute, attribute) == 0) {
                int oldFieldSize = field->size;
                if (field->type == VARSTR || field->type == STR) {
                    free(field->stringValue);
                }
                updateField(field, queryValues->values[i]);
                if (field->type == VARSTR) {
                    record->size += field->size - oldFieldSize;
                }
            }
        }
    }
    outputRecord(record);

    if (record->size <= oldSize) {
        LOG("Update record in place\n");
        outputRecord(record);
        uint16_t recordStart = page->header->recordStart - record->size;
        writeRecord(page->ptr + recordStart, record);
        updatePageHeaderUpdate(page, record, oldSize, iterator->lastSlot,
                               recordStart);
        Record newRecord = parseRecord(page->ptr + recordStart, schema);
        LOG("New record\n");
        outputRecord(newRecord);
        defragmentRecords(iterator->page);
        updatePage(tableInfo, page);
        freeRecord(newRecord);
    } else {
        LOG("Relocate record\n");
        outputRecord(record);

        Condition condition = malloc(sizeof(struct Condition));
        // condition->value.twoArg.op1 = GLOBAL_ID_NAME;
        condition->value.twoArg.op2 =
            createOperand(INT, (uint8_t *)&record->globalIdx);
        condition->type = EQUALS;
        LOG("Deleting entries with global idx %d\n", record->globalIdx);
        deleteFrom(tableInfo, spaceMap, schema, condition);
        extendedDisplayTable(tableInfo->name, RELATION);
        // Insertion will find new page in which to allocate record
        // If record is iterated again, then it will be rewritten in place
        // with no further reallocation
        insertRecord(tableInfo, spaceMap, schema, record, RELATION);
        free(condition);
    }
}

void updateTable(TableInfo tableInfo, TableInfo spaceMap,
                 QueryAttributes queryAttributes, QueryValues queryValues,
                 Condition cond, Schema *schema) {
    struct RecordIterator iterator;
    initialiseRecordIterator(&iterator);

    // Iterates through records, updating those that satisfy condition
    Record record = iterateRecords(tableInfo, schema, &iterator, true);
    while (record != NULL) {
        outputRecord(record);
        if (evaluate(record, cond)) {
            updateRecord(tableInfo, schema, spaceMap, record, iterator.page,
                         queryAttributes, queryValues, &iterator);
        }
        freeRecord(record);
        record = iterateRecords(tableInfo, schema, &iterator, true);
    }
    freeRecordIterator(&iterator);
}

void updateOperation(TableInfo table, TableInfo spaceMap, Schema *schema,
                     Operation operation) {
    updateTable(table, spaceMap, operation->query.update.attributes,
                operation->query.update.values,
                operation->query.update.condition, schema);
}
