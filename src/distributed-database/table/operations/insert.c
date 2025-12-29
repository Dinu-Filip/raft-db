#include "insert.h"

#include <assert.h>
#include <stdlib.h>

#include "../core/pages.h"
#include "../core/record.h"
#include "log.h"
#include "table/core/table.h"

void insertInto(TableInfo tableInfo, TableInfo spaceMap, Schema *schema,
                QueryAttributes attributes, QueryValues values,
                TableType type) {
    // Creates record from query
    Record record =
        parseQuery(schema, attributes, values, tableInfo->header->globalIdx);

    insertRecord(tableInfo, spaceMap, record, type);
    freeRecord(record);
}

void insertRecord(TableInfo tableInfo, TableInfo spaceMap, Record record, TableType type) {
    Page page = nextFreePage(tableInfo, spaceMap, record->size, type);

    // Increment global index
    tableInfo->header->globalIdx++;
    tableInfo->header->modified = true;

    // Calculates position at which to insert record
    uint16_t recordStart = page->header->recordStart - record->size;
    writeRecord(
        page->ptr + recordStart, record);

    updatePageHeaderInsert(record, page, recordStart);
    updatePage(tableInfo, page);

    if (type == RELATION) {
        // Updates free space in page if table is a relation
        updateSpaceInventory(tableInfo->name, spaceMap, page);
    }

    freePage(page);
    updateTableHeader(tableInfo);
}

void insertOperation(TableInfo tableInfo, TableInfo spaceMap, Schema *schema,
                     Operation operation, TableType type) {
    insertInto(tableInfo, spaceMap, schema, operation->query.insert.attributes,
               operation->query.insert.values, type);
}