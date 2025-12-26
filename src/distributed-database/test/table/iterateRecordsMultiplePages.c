#include "iterateRecordsMultiplePages.h"

#include "multiplePageDummy.h"
#include "table/core/field.h"
#include "table/core/record.h"
#include "table/core/table.h"
#include "test-library.h"

void testIteratorRecordsMultiplePages() {
    createMultiplePageDummy();

    Schema schema;
    AttributeName names[7] = {"id", "age", "height", "student", "num", "email", "name"};
    AttributeType types[7] = {INT, INT, FLOAT, BOOL, INT, VARSTR, VARSTR};
    unsigned sizes[7] = {INT_WIDTH, INT_WIDTH, FLOAT_WIDTH, BOOL_WIDTH, INT_WIDTH, 50, 50};

    schema.attributes = names;
    schema.attributeTypes = types;
    schema.attributeSizes = sizes;
    schema.numAttributes = 7;

    TableInfo table = openTable("testdb");
    struct RecordIterator iterator;
    initialiseRecordIterator(&iterator);
    unsigned expected = 0;

    Record record = iterateRecords(table, &schema, &iterator, true);

    while (record != NULL) {
        ASSERT_EQ(record->fields[0].intValue, expected);
        ASSERT_STR_EQ(record->fields[6].stringValue, "Dinu");
        expected++;
        record = iterateRecords(table, &schema, &iterator, true);
    }

    ASSERT_EQ(expected, 500);
    FINISH_OUTER_TEST
    PRINT_SUMMARY

}