#ifndef RECORD_H
#define RECORD_H

#include "table/table.h"

Record parseQuery(Schema *schema, QueryAttributes attributes, QueryValues values, uint32_t globalIdx);

/**
 * Parses raw bytes in file into Record
 * @param page page to read from
 * @param offset offset to record
 * @param schema schema for parsing
 */
extern Record parseRecord(Page page, size_t offset, Schema *schema);
#endif //RECORD_H
