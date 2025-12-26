#include "recordParseVarLength.h"

#include <assert.h>

#include "../../../lib/test-library.h"
#include "../../table/core/field.h"
#include "../../table/core/record.h"
#include "../../table/operations/operation.h"
#include "../../table/schema.h"

void testRecordParseVarLength() {
    Schema schema;
    AttributeName names[6] = {"age", "height", "student", "num", "email", "name"};
    AttributeType types[6] = {INT, FLOAT, BOOL, INT, VARSTR, VARSTR};
    unsigned sizes[6] = {INT_WIDTH, FLOAT_WIDTH, BOOL_WIDTH, INT_WIDTH, 50, 50};
    schema.attributes = names;
    schema.attributeTypes = types;
    schema.attributeSizes = sizes;
    schema.numAttributes = 6;

    struct Record record;
    Field fields[6] = {
        {.attribute = "age", .type = INT, .size = INT_WIDTH, .intValue = 20},
        {.attribute = "height", .type = FLOAT, .size = FLOAT_WIDTH, .floatValue = 194.5},
        {.attribute = "student", .type = BOOL, .size = BOOL_WIDTH, .boolValue = true},
        {.attribute = "num", .type = INT, .size = INT_WIDTH, .intValue = -10},
        {.attribute = "email", .type = VARSTR, .size = 25, .stringValue = "dinu.filip.self@gmail.com"},
        {.attribute = "name", .type = VARSTR, .size = 4, .stringValue = "Dinu"},

    };
    record.fields = fields;
    record.numValues = 6;
    record.size = RECORD_HEADER_WIDTH + SLOT_SIZE * 2 + GLOBAL_ID_WIDTH + INT_WIDTH + FLOAT_WIDTH + BOOL_WIDTH + INT_WIDTH + 25 + 4;
    record.globalIdx = 6;

    uint8_t *ptr = calloc(record.size, sizeof(uint8_t));
    assert(ptr != NULL);

    writeRecord(ptr, &record);
    Record res = parseRecord(ptr, &schema);

    START_OUTER_TEST("Test record parsing and serialisation with variable length fields")

    ASSERT_EQ(20, res->fields[0].intValue);
    ASSERT_EQ(194.5, res->fields[1].floatValue);
    ASSERT_EQ(true, res->fields[2].boolValue);
    ASSERT_EQ(-10, res->fields[3].intValue);
    ASSERT_STR_EQ("dinu.filip.self@gmail.com", res->fields[4].stringValue);
    ASSERT_STR_EQ("Dinu", res->fields[5].stringValue);

    PRINT_SUMMARY
}