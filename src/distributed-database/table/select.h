#ifndef SELECT_H
#define SELECT_H

#include "table.h"
#include "table/operation.h"

extern QueryResult selectOperation(TableInfo tableInfo, Schema *schema,
                                   Operation operation);

extern QueryResult selectFrom(TableInfo tableInfo, Schema *schema,
                              Condition cond, QueryAttributes attributes);

#endif  // SELECT_H
