#ifndef DELETE_H
#define DELETE_H

#include "table.h"

extern void deleteFrom(TableInfo table, TableInfo spaceMap, Schema *schema,
                       Condition condition);

extern void deleteOperation(TableInfo table, TableInfo spaceMap, Schema *schema,
                            Operation operation);

#endif  // DELETE_H
