#include "recordParseVarLength.h"

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

void testRecordParseVarLength() {
    Schema schema;

    schema.numAttrs = 6;
    schema.attrInfos = malloc(sizeof(AttrInfo) * schema.numAttrs);

    Schema *s = &schema;
    ATTR_CREATE(s, 0, "age", INT, INT_WIDTH, 0);
    ATTR_CREATE(s, 1, "height", FLOAT, FLOAT_WIDTH, INT_WIDTH);
    ATTR_CREATE(s, 2, "student", BOOL, BOOL_WIDTH, INT_WIDTH + FLOAT_WIDTH);
    ATTR_CREATE(s, 3, "num", INT, INT_WIDTH,
                INT_WIDTH + FLOAT_WIDTH + BOOL_WIDTH);
    ATTR_CREATE(s, 4, "email", VARSTR, 50, 0);
    ATTR_CREATE(s, 5, "name", VARSTR, 50, 1);

    struct Record record;
    Field fields[6] = {
        {.attribute = "age", .type = INT, .size = INT_WIDTH, .intValue = 20},
        {.attribute = "height",
         .type = FLOAT,
         .size = FLOAT_WIDTH,
         .floatValue = 194.5},
        {.attribute = "student",
         .type = BOOL,
         .size = BOOL_WIDTH,
         .boolValue = true},
        {.attribute = "num", .type = INT, .size = INT_WIDTH, .intValue = -10},
        {.attribute = "email",
         .type = VARSTR,
         .size = 25,
         .stringValue = "dinu.filip.self@gmail.com"},
        {.attribute = "name", .type = VARSTR, .size = 4, .stringValue = "Dinu"},

    };
    record.fields = fields;
    record.numValues = 6;
    record.size = RECORD_HEADER_WIDTH + OFFSET_WIDTH * 3 + GLOBAL_ID_WIDTH +
                  INT_WIDTH + FLOAT_WIDTH + BOOL_WIDTH + INT_WIDTH + 25 + 4;
    record.globalIdx = 6;

    uint8_t *ptr = calloc(record.size, sizeof(uint8_t));
    assert(ptr != NULL);

    writeRecord(ptr, &record);
    Record res = parseRecord(ptr, &schema);

    START_OUTER_TEST(
        "Test record parsing and serialisation with variable length fields")

    ASSERT_EQ(20, res->fields[0].intValue);
    ASSERT_EQ(194.5, res->fields[1].floatValue);
    ASSERT_EQ(true, res->fields[2].boolValue);
    ASSERT_EQ(-10, res->fields[3].intValue);
    ASSERT_STR_EQ("dinu.filip.self@gmail.com", res->fields[4].stringValue);
    ASSERT_STR_EQ("Dinu", res->fields[5].stringValue);

    PRINT_SUMMARY
}