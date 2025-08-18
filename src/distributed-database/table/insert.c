#include "insert.h"

#include <assert.h>
#include <stdlib.h>

#include "db-utils.h"
#include "log.h"
#include "pages.h"
#include "table/table.h"

void updatePageHeaderInsert(Record record, Page page, uint16_t recordStart) {
    LOG("Update page header insert\n");

    // Updates free space log in page header
    page->header->freeSpace -= record->size;

    page->header->modified = true;
    page->header->numRecords++;
    // Record always inserted at beginning of free space
    page->header->recordStart = recordStart;

    int idx = 0;
    RecordSlot *temp;

    // Iterates through record slots until empty slot with 0 size is found
    // (after a deletion) or end of list is reached, in which case tail is
    // shifted one place
    do {
        temp = &page->header->recordSlots[idx++];
        if (temp->size == PAGE_TAIL) {
            RecordSlot *newTail = &page->header->recordSlots[idx];
            newTail->size = PAGE_TAIL;
            newTail->modified = true;

            // Total free space subtracted for new slot
            page->header->freeSpace -= OFFSET_WIDTH + SIZE_WIDTH;
        }
        if (temp->size == 0 || temp->size == PAGE_TAIL) {
            temp->offset = recordStart;

            assert(record->size <= INT16_MAX);
            temp->size = record->size;

            temp->modified = true;
            break;
        }
    } while (temp->size != PAGE_TAIL);
}

void insertInto(TableInfo tableInfo, TableInfo spaceMap, Schema *schema,
                QueryAttributes attributes, QueryValues values,
                TableType type) {
    // Assumed no null or missing values
    Record record =
        parseQuery(schema, attributes, values, tableInfo->header->globalIdx);

    outputRecord(record);
    insertRecord(tableInfo, spaceMap, schema, record, type);
    freeRecord(record);
}

void insertRecord(TableInfo tableInfo, TableInfo spaceMap, Schema *schema,
                  Record record, TableType type) {
    outputRecord(record);
    Page page = nextFreePage(tableInfo, spaceMap, record->size, type);

    // Increment global index
    tableInfo->header->globalIdx++;
    tableInfo->header->modified = true;
    LOG("GLOBAL INDEX: %hu", tableInfo->header->globalIdx);

    uint16_t recordStart = writeRecord(
        page, record, tableInfo->header->globalIdx, page->header->recordStart);
    updatePageHeaderInsert(record, page, recordStart);
    updatePage(tableInfo, page);

    if (type == RELATION) {
        // Updates free space in page if table is a relation
        updateSpaceInventory(tableInfo->name, spaceMap, page);
    }

    outputRecord(record);
    freePage(page);
    updateTableHeader(tableInfo);
}

void insertOperation(TableInfo tableInfo, TableInfo spaceMap, Schema *schema,
                     Operation operation) {
    insertInto(tableInfo, spaceMap, schema, operation->query.insert.attributes,
               operation->query.insert.values, RELATION);
}
