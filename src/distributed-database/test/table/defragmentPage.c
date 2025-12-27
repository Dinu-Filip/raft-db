#include "defragmentPage.h"

#include "networking/msg.h"
#include "singlePageDummy.h"
#include "table/core/pages.h"
#include "table/core/record.h"
#include "table/core/table.h"
#include "test-library.h"

void testDefragmentPage() {
    createSinglePageDummy();

    Schema schema;
    AttributeName names[7] = {"id", "age", "height", "student", "num", "email", "name"};
    AttributeType types[7] = {INT, INT, FLOAT, BOOL, INT, VARSTR, VARSTR};
    unsigned sizes[7] = {INT_WIDTH, INT_WIDTH, FLOAT_WIDTH, BOOL_WIDTH, INT_WIDTH, 50, 50};

    schema.attributes = names;
    schema.attributeTypes = types;
    schema.attributeSizes = sizes;
    schema.numAttributes = 7;

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
    Record record = iterateRecords(table, &schema, &iterator, false);
    unsigned expected = 0;

    while (record != NULL) {
        offset -= record->size;
        ASSERT_EQ(iterator.lastSlot->offset, offset);
        expected++;
        record = iterateRecords(table, &schema, &iterator, false);
    }

    page = getPage(table, 1);

    ASSERT_EQ(page->header->freeSpace, _PAGE_SIZE - 3 * NUM_SLOTS_WIDTH - 50 * SLOT_SIZE - 25 * recordSize);
    ASSERT_EQ(expected, 25);

    FINISH_OUTER_TEST
    PRINT_SUMMARY
}