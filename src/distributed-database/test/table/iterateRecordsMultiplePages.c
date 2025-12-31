#include "iterateRecordsMultiplePages.h"

#include "multiplePageDummy.h"
#include "table/core/field.h"
#include "table/core/record.h"
#include "table/core/table.h"
#include "test-library.h"

#define ATTR_CREATE(schema, idx, name_, type_, size_, loc_) \
    ({                                                      \
        schema->attrInfos[idx].name = name_;                \
        schema->attrInfos[idx].type = type_;                \
        schema->attrInfos[idx].size = size_;                \
        schema->attrInfos[idx].loc = loc_;                  \
    })

void testIteratorRecordsMultiplePages() {
    createMultiplePageDummy();

    Schema schema;

    schema.numAttrs = 7;
    schema.attrInfos = malloc(sizeof(AttrInfo) * schema.numAttrs);

    Schema *s = &schema;
    ATTR_CREATE(s, 0, "id", INT, INT_WIDTH, 0);
    ATTR_CREATE(s, 1, "age", INT, INT_WIDTH, INT_WIDTH);
    ATTR_CREATE(s, 2, "height", FLOAT, FLOAT_WIDTH, INT_WIDTH * 2);
    ATTR_CREATE(s, 3, "student", BOOL, BOOL_WIDTH, INT_WIDTH * 2 + FLOAT_WIDTH);
    ATTR_CREATE(s, 4, "num", INT, INT_WIDTH, INT_WIDTH * 2 + FLOAT_WIDTH + BOOL_WIDTH);
    ATTR_CREATE(s, 5, "email", VARSTR, 50, 0);
    ATTR_CREATE(s, 6, "name", VARSTR, 50, 1);

    TableInfo table = openTable("testdb");
    struct RecordIterator iterator;
    initialiseRecordIterator(&iterator);
    unsigned expected = 0;

    bool canContinue = iterateRecords(table, &iterator, true);

    while (canContinue) {
        Record record = parseRecord(iterator.page->ptr + iterator.lastSlot->offset, &schema);
        ASSERT_EQ(record->fields[0].intValue, expected);
        ASSERT_STR_EQ(record->fields[6].stringValue, "Dinu");
        expected++;
        canContinue = iterateRecords(table, &iterator, true);
    }

    ASSERT_EQ(expected, 500);
    FINISH_OUTER_TEST
    PRINT_SUMMARY
}