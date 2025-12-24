#ifndef RECORD_H
#define RECORD_H

#include "table/table.h"

Record parseQuery(Schema *schema, QueryAttributes attributes, QueryValues values, uint32_t globalIdx);

#endif //RECORD_H
