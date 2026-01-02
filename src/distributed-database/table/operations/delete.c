#include "../conditions.h"
#include "../core/pages.h"
#include "../core/record.h"
#include "table/core/table.h"
#include "table/operations/operation.h"

void deleteFrom(TableInfo table, TableInfo spaceMap, Schema *schema,
                Condition condition) {
    struct RecordIterator iterator;
    initialiseRecordIterator(&iterator);

    bool canIterate = iterateRecords(table, &iterator, false);
    while (canIterate) {
        Record record = parseRecord(iterator.page->ptr + iterator.lastSlot->offset, schema);
        if (evaluate(record, condition)) {
            removeRecord(iterator.page, iterator.lastSlot, record->size);
        }

        freeRecord(record);
        Page oldPage = iterator.page;
        canIterate = iterateRecords(table, &iterator, false);

        // Defragments records from deleted page only when page is left
        if (!canIterate || iterator.page->pageId != oldPage->pageId) {
            defragmentRecords(oldPage);
            updatePage(table, oldPage);
            updateSpaceInventory(spaceMap, oldPage);
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
