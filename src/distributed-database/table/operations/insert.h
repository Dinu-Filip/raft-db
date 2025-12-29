#ifndef INSERT_H
#define INSERT_H

#include "../core/table.h"
#include "table/operations/operation.h"

extern void insertOperation(TableInfo tableInfo, TableInfo spaceMap,
                            Schema *schema, Operation operation, TableType type);

extern void insertInto(TableInfo tableInfo, TableInfo spaceMap, Schema *schema,
                       QueryAttributes attributes, QueryValues values,
                       TableType type);

extern void insertRecord(TableInfo tableInfo, TableInfo spaceMap, Record record, TableType type);

#endif  // INSERT_H
