#ifndef DELETE_H
#define DELETE_H

#include "table.h"

extern void deleteFrom(TableInfo table, Schema *schema, Condition condition);

extern void deleteOperation(TableInfo table, Schema *schema, Operation operation);

#endif  // DELETE_H
