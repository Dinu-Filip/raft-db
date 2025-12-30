#include "recordParseFixedLength.h"

#include <assert.h>

#include "../../../lib/test-library.h"
#include "../../table/core/field.h"
#include "../../table/core/record.h"
#include "../../table/operations/operation.h"
#include "../../table/schema.h"

#define ATTR_CREATE(schema, idx, name_, type_, size_, loc_) \
    ({                                                      \
        schema->attrInfos[idx].name = name_;                \
        schema->attrInfos[idx].type = type_;                \
        schema->attrInfos[idx].size = size_;                \
        schema->attrInfos[idx].loc = loc_;                  \
    })

void testRecordParseFixedLength() {
    Schema schema;

    schema.numAttrs = 7;
    schema.attrInfos = malloc(sizeof(AttrInfo) * schema.numAttrs);

    Schema *s = &schema;
    ATTR_CREATE(s, 0, "name", STR, STR_WIDTH, 0);
    ATTR_CREATE(s, 1, "age", INT, INT_WIDTH, STR_WIDTH);
    ATTR_CREATE(s, 2, "height", FLOAT, FLOAT_WIDTH, INT_WIDTH + STR_WIDTH);
    ATTR_CREATE(s, 3, "student", BOOL, BOOL_WIDTH,
         INT_WIDTH + FLOAT_WIDTH + STR_WIDTH);
    ATTR_CREATE(s, 4, "num", INT, INT_WIDTH,
         INT_WIDTH + FLOAT_WIDTH + BOOL_WIDTH + STR_WIDTH);

    struct Record record;
    Field fields[5] = {
        {.attribute = "name", .type = STR, .size = 4, .stringValue = "Dinu"},
        {.attribute = "age", .type = INT, .size = INT_WIDTH, .intValue = 20},
        {.attribute = "height",
         .type = FLOAT,
         .size = FLOAT_WIDTH,
         .floatValue = 194.5},
        {.attribute = "student",
         .type = BOOL,
         .size = BOOL_WIDTH,
         .boolValue = true},
        {.attribute = "num", .type = INT, .size = INT_WIDTH, .intValue = -10}};
    record.fields = fields;
    record.numValues = 5;
    record.size = RECORD_HEADER_WIDTH + GLOBAL_ID_WIDTH + 4 + INT_WIDTH +
                  FLOAT_WIDTH + BOOL_WIDTH + INT_WIDTH;
    record.globalIdx = 6;

    uint8_t *ptr = calloc(record.size, sizeof(uint8_t));
    assert(ptr != NULL);

    writeRecord(ptr, &record);
    Record res = parseRecord(ptr, &schema);

    START_OUTER_TEST(
        "Test record parsing and serialisation with static length fields")

    ASSERT_STR_EQ("Dinu", res->fields[0].stringValue);
    ASSERT_EQ(20, res->fields[1].intValue);
    ASSERT_EQ(194.5, res->fields[2].floatValue);
    ASSERT_EQ(true, res->fields[3].boolValue);
    ASSERT_EQ(-10, res->fields[4].intValue);

    PRINT_SUMMARY
}