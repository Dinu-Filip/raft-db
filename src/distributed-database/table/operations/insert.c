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

    uint16_t recordStart = page->header->recordStart - record->size;
    writeRecord(
        page->ptr + recordStart, record);
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
