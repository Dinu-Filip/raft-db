#ifndef SELECT_H
#define SELECT_H

#include "../core/table.h"
#include "table/operations/operation.h"
#include "table/schema.h"

extern QueryResult selectOperation(TableInfo tableInfo, Schema *schema,
                                   Operation operation);

extern QueryResult selectFrom(TableInfo tableInfo, Schema *schema,
                              Condition cond, QueryAttributes attributes);

#endif  // SELECT_H
