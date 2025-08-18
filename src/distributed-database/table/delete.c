#include <stdlib.h>

#include "conditions.h"
#include "pages.h"
#include "table/operation.h"
#include "table/table.h"

static void removeRecord(char *tableName, TableInfo spaceMap, Page page,
                         RecordSlot *slot, size_t recordSize) {
    slot->size = 0;
    slot->modified = true;
    page->header->modified = true;
    page->header->freeSpace += recordSize + SLOT_SIZE;
    page->header->numRecords--;

    // if (spaceMap != NULL) {
    //     updateSpaceInventory(tableName, spaceMap, page);
    // }
}

void deleteFrom(TableInfo table, TableInfo spaceMap, Schema *schema,
                Condition condition) {
    RecordIterator iterator;
    initialiseRecordIterator(&iterator);

    Record record = iterateRecords(table, schema, &iterator, false);
    while (record != NULL) {
        outputRecord(record);
        if (evaluate(record, condition)) {
            removeRecord(table->name, spaceMap, iterator.page,
                         iterator.lastSlot, record->size);
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
    freeRecordIterator(iterator);
}

void deleteOperation(TableInfo table, TableInfo spaceMap, Schema *schema,
                     Operation operation) {
    deleteFrom(table, spaceMap, schema, operation->query.delete.condition);
}
