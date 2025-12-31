#include "defragmentPage.h"

#include "networking/msg.h"
#include "singlePageDummy.h"
#include "table/core/pages.h"
#include "table/core/record.h"
#include "table/core/table.h"
#include "test-library.h"

#define CREATE_ATTR(schema, idx, name_, type_, size_, loc_) ({\
    schema->attrInfos[idx].name = name_; \
    schema->attrInfos[idx].type = type_;\
    schema->attrInfos[idx].size = size_;\
    schema->attrInfos[idx].loc = loc_;\
})

void testDefragmentPage() {
    createSinglePageDummy();

    Schema schema;

    schema.numAttrs = 7;
    schema.attrInfos = malloc(sizeof(AttrInfo) * schema.numAttrs);

    Schema *s = &schema;
    CREATE_ATTR(s, 0, "id", INT, INT_WIDTH, 0);
    CREATE_ATTR(s, 1, "age", INT, INT_WIDTH, INT_WIDTH);
    CREATE_ATTR(s, 2, "height", FLOAT, FLOAT_WIDTH, INT_WIDTH * 2);
    CREATE_ATTR(s, 3, "student", BOOL, BOOL_WIDTH, INT_WIDTH * 2 + FLOAT_WIDTH);
    CREATE_ATTR(s, 4, "num", INT, INT_WIDTH, INT_WIDTH * 2 + FLOAT_WIDTH + BOOL_WIDTH);
    CREATE_ATTR(s, 5, "email", VARSTR, 50, 0);
    CREATE_ATTR(s, 6, "name", VARSTR, 50, 1);

    TableInfo table = openTable("testdb");
    Page page = getPage(table, 1);
    RecordSlot *slots = page->header->slots.slots;
    page->header->modified = true;
    for (int i = 0; i < page->header->slots.size; i += 2) {
        slots[i].size = 0;
        slots[i].offset = 0;
        slots[i].modified = true;
    }

    page->header->numRecords = 25;

    defragmentRecords(page);

    updatePage(table, page);
    unsigned recordSize = page->header->slots.slots[1].size;

    freePage(page);

    START_OUTER_TEST("Test defragment a single page")

    struct RecordIterator iterator;
    initialiseRecordIterator(&iterator);

    unsigned offset = _PAGE_SIZE;
    bool canContinue = iterateRecords(table, &iterator, false);
    unsigned expected = 0;

    while (canContinue) {
        Record record = parseRecord(iterator.page->ptr + iterator.lastSlot->offset, &schema);
        offset -= record->size;
        ASSERT_EQ(iterator.lastSlot->offset, offset);
        ASSERT_STR_EQ(record->fields[6].stringValue, "Dinu")
        expected++;
        canContinue = iterateRecords(table, &iterator, false);
    }

    page = getPage(table, 1);

    ASSERT_EQ(page->header->freeSpace, _PAGE_SIZE - 3 * NUM_SLOTS_WIDTH - 50 * SLOT_SIZE - 25 * recordSize);
    ASSERT_EQ(expected, 25);

    FINISH_OUTER_TEST
    PRINT_SUMMARY
}