#include "recordParseFixedLength.h"

#include <assert.h>

#include "../../../lib/test-library.h"
#include "../../table/core/field.h"
#include "../../table/core/record.h"
#include "../../table/operations/operation.h"
#include "../../table/schema.h"

void testRecordParseFixedLength() {
    Schema schema;
    AttributeName names[5] = {"name", "age", "height", "student", "num"};
    AttributeType types[5] = {STR, INT, FLOAT, BOOL, INT};
    unsigned sizes[5] = {4, INT_WIDTH, FLOAT_WIDTH, BOOL_WIDTH, INT_WIDTH};
    schema.attributes = names;
    schema.attributeTypes = types;
    schema.attributeSizes = sizes;
    schema.numAttributes = 5;

    struct Record record;
    Field fields[5] = {
        {.attribute = "name", .type = STR, .size = 4, .stringValue = "Dinu"},
        {.attribute = "age", .type = INT, .size = INT_WIDTH, .intValue = 20},
        {.attribute = "height", .type = FLOAT, .size = FLOAT_WIDTH, .floatValue = 194.5},
        {.attribute = "student", .type = BOOL, .size = BOOL_WIDTH, .boolValue = true},
        {.attribute = "num", .type = INT, .size = INT_WIDTH, .intValue = -10}
    };
    record.fields = fields;
    record.numValues = 5;
    record.size = RECORD_HEADER_WIDTH + GLOBAL_ID_WIDTH + 4 + INT_WIDTH + FLOAT_WIDTH + BOOL_WIDTH + INT_WIDTH;
    record.globalIdx = 6;

    uint8_t *ptr = calloc(record.size, sizeof(uint8_t));
    assert(ptr != NULL);

    writeRecord(ptr, &record);
    Record res = parseRecord(ptr, &schema);

    START_OUTER_TEST("Test record parsing and serialisation with static length fields")

    ASSERT_STR_EQ("Dinu", res->fields[0].stringValue);
    ASSERT_EQ(20, res->fields[1].intValue);
    ASSERT_EQ(194.5, res->fields[2].floatValue);
    ASSERT_EQ(true, res->fields[3].boolValue);
    ASSERT_EQ(-10, res->fields[4].intValue);

    PRINT_SUMMARY
}