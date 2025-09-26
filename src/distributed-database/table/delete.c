#include <stdlib.h>

#include "buffer/pageBuffer.h"
#include "conditions.h"
#include "table/operation.h"
#include "table/table.h"

static void removeRecord(Page page, RecordSlot *slot, size_t recordSize) {
    slot->size = 0;
    slot->modified = true;

    // Offset to start of records is shifted forward by size of record
    // Free space is increased by size of record and freed up slot
    setPageHeader(page, page->header->numRecords - 1,
            page->header->recordStart + recordSize,
            page->header->freeSpace + recordSize + SLOT_SIZE);

    // if (spaceMap != NULL) {
    //     updateSpaceInventory(tableName, spaceMap, page);
    // }
}

void deleteFrom(TableInfo table, Schema *schema, Condition condition) {
    RecordIterator iterator;
    initialiseRecordIterator(&iterator);

    Record record = iterateRecords(table, schema, &iterator, false);

    while (record != NULL) {
        // Removes record only if satisfies condition
        if (evaluate(record, condition)) {
            removeRecord(iterator.page, iterator.lastSlot, record->size);
        }
        freeRecord(record);
        Page oldPage = iterator.page;
        record = iterateRecords(table, schema, &iterator, false);

        // Defragments records from deleted page only when page is left
        if (record == NULL || iterator.page->pageId != oldPage->pageId) {
            defragmentRecords(oldPage);
        }
    }

    updateTableHeader(table);
    freeRecordIterator(iterator);
}

void deleteOperation(TableInfo table, Schema *schema, Operation operation) {
    deleteFrom(table, schema, operation->query.delete.condition);
}
