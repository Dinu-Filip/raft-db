#include "singlePageDummy.h"

#include "table/core/field.h"
#include "table/core/pages.h"
#include "table/core/record.h"
#include "table/core/table.h"
#include "test-library.h"

static void addRecord(Page page, int id) {
    struct Record record;
    Field fields[7] = {
        {.attribute = "id", .type = INT, .size = INT_WIDTH, .intValue = id},
        {.attribute = "age", .type = INT, .size = INT_WIDTH, .intValue = 20},
        {.attribute = "height", .type = FLOAT, .size = FLOAT_WIDTH, .floatValue = 194.5},
        {.attribute = "student", .type = BOOL, .size = BOOL_WIDTH, .boolValue = true},
        {.attribute = "num", .type = INT, .size = INT_WIDTH, .intValue = -10},
        {.attribute = "email", .type = VARSTR, .size = 25, .stringValue = "dinu.filip.self@gmail.com"},
        {.attribute = "name", .type = VARSTR, .size = 4, .stringValue = "Dinu"},
    };

    record.fields = fields;
    record.numValues = 7;
    record.size = RECORD_HEADER_WIDTH + OFFSET_WIDTH * 3 + GLOBAL_ID_WIDTH + INT_WIDTH + INT_WIDTH + FLOAT_WIDTH + BOOL_WIDTH + INT_WIDTH + 25 + 4;
    record.globalIdx = 6;

    uint16_t startOffset = page->header->recordStart - record.size;

    writeRecord(page->ptr + startOffset, &record);
    updatePageHeaderInsert(&record, page, startOffset);
}

void createSinglePageDummy() {
    initialiseTable("testdb");
    TableInfo info = openTable("testdb");

    Page page = addPage(info);

    for (int i = 0; i < 50; i++) {
        addRecord(page, i);
    }

    updatePage(info, page);

    closeTable(info);
}