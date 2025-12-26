#include "recordParseFixedLength.h"

#include <assert.h>

#include "../../../lib/test-library.h"
#include "../../table/core/field.h"
#include "../../table/core/record.h"
#include "../../table/operations/operation.h"
#include "../../table/schema.h"

int main() {
    Schema schema;
    AttributeName names[4] = {"name", "age", "height", "student"};
    AttributeType types[4] = {STR, INT, FLOAT, BOOL};
    unsigned sizes[4] = {4, INT_WIDTH, FLOAT_WIDTH, BOOL_WIDTH};
    schema.attributes = names;
    schema.attributeTypes = types;
    schema.attributeSizes = sizes;
    schema.numAttributes = 4;

    struct Record record;
    Field fields[4] = {
        {.attribute = "name", .type = STR, .size = 4, .stringValue = "Dinu"},
        {.attribute = "age", .type = INT, .size = INT_WIDTH, .intValue = 20},
        {.attribute = "height", .type = FLOAT, .size = FLOAT_WIDTH, .floatValue = 194.5},
        {.attribute = "student", .type = BOOL, .size = BOOL_WIDTH, .boolValue = true}
    };
    record.fields = fields;
    record.numValues = 4;
    record.size = RECORD_HEADER_WIDTH + GLOBAL_ID_WIDTH + 4 + INT_WIDTH + FLOAT_WIDTH + BOOL_WIDTH;
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

    PRINT_SUMMARY
}