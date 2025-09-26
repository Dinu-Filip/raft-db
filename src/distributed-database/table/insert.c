#include "insert.h"

#include <assert.h>
#include <stdlib.h>

#include "buffer/pageBuffer.h"
#include "log.h"
#include "table/table.h"

static updateSlots(PageHeader pageHeader, uint16_t recordStart,
                   size_t recordSize) {
    int idx = 0;
    RecordSlot *temp;

    // Iterates through record slots until empty slot with 0 size is found
    // (after a deletion) or end of list is reached, in which case tail is
    // shifted one place
    do {
        temp = &pageHeader->recordSlots[idx++];
        if (temp->size == PAGE_TAIL) {
            RecordSlot *newTail = &pageHeader->recordSlots[idx];
            newTail->size = PAGE_TAIL;
            newTail->modified = true;

            // Total free space subtracted for new slot
            pageHeader->freeSpace -= OFFSET_WIDTH + SIZE_WIDTH;
        }
        if (temp->size == 0 || temp->size == PAGE_TAIL) {
            temp->offset = recordStart;

            assert(recordSize <= INT16_MAX);
            temp->size = recordSize;

            temp->modified = true;
            break;
        }
    } while (temp->size != PAGE_TAIL);
}

void updatePageHeaderInsert(Record record, Page page, uint16_t recordStart) {
    LOG("Update page header insert\n");

    // Updates free space log in page header
    PageHeader pageHeader = page->header;
    setPageHeader(page, pageHeader->numRecords + 1, pageHeader->recordStart,
                  pageHeader->freeSpace - record->size);
    updateSlots(page->header, recordStart, record->size);
}

void insertInto(TableInfo tableInfo, TableInfo spaceMap, Schema *schema,
                QueryAttributes attributes, QueryValues values,
                TableType type) {
    // Assumed no null or missing values
    Record record =
        parseQuery(schema, attributes, values, tableInfo->header->globalIdx);
    insertRecord(tableInfo, spaceMap, schema, record, type);
    freeRecord(record);
}

void insertRecord(TableInfo tableInfo, TableInfo spaceMap, Schema *schema,
                  Record record, TableType type) {
    Page page = nextFreePage(tableInfo, spaceMap, record->size, type);

    // Increment global index
    tableInfo->header->globalIdx++;
    tableInfo->header->modified = true;
    LOG("GLOBAL INDEX: %hu", tableInfo->header->globalIdx);

    uint16_t recordStart = writeRecord(
        page, record, tableInfo->header->globalIdx, page->header->recordStart);
    updatePageHeaderInsert(record, page, recordStart);

    if (type == RELATION) {
        // Updates free space in page if table is a relation
        updateSpaceInventory(tableInfo->name, spaceMap, page);
    }

    freePage(page);
    updateTableHeader(tableInfo);
}

void insertOperation(TableInfo tableInfo, TableInfo spaceMap, Schema *schema,
                     Operation operation) {
    insertInto(tableInfo, spaceMap, schema, operation->query.insert.attributes,
               operation->query.insert.values, RELATION);
}
