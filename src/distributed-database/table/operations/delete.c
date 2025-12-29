#include <stdlib.h>

#include "../conditions.h"
#include "../core/pages.h"
#include "../core/record.h"
#include "table/core/table.h"
#include "table/operations/operation.h"

void deleteFrom(TableInfo table, TableInfo spaceMap, Schema *schema,
                Condition condition) {
    struct RecordIterator iterator;
    initialiseRecordIterator(&iterator);

    Record record = iterateRecords(table, schema, &iterator, false);
    while (record != NULL) {

        if (evaluate(record, condition)) {
            removeRecord(iterator.page, iterator.lastSlot, record->size);
        }

        freeRecord(record);
        Page oldPage = iterator.page;
        record = iterateRecords(table, schema, &iterator, false);

        // Defragments records from deleted page only when page is left
        if (record == NULL || iterator.page->pageId != oldPage->pageId) {
            defragmentRecords(oldPage);
            updatePage(table, oldPage);
            freePage(oldPage);
        }
    }

    updateTableHeader(table);
    freeRecordIterator(&iterator);
}

void deleteOperation(TableInfo table, TableInfo spaceMap, Schema *schema,
                     Operation operation) {
    deleteFrom(table, spaceMap, schema, operation->query.delete.condition);
}
