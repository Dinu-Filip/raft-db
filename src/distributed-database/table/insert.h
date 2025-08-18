#ifndef INSERT_H
#define INSERT_H

#include "table.h"
#include "table/operation.h"

extern void insertOperation(TableInfo tableInfo, TableInfo spaceMap,
                            Schema *schema, Operation operation);

extern void insertInto(TableInfo tableInfo, TableInfo spaceMap, Schema *schema,
                       QueryAttributes attributes, QueryValues values,
                       TableType type);

extern void insertRecord(TableInfo tableInfo, TableInfo spaceMap,
                         Schema *schema, Record record, TableType type);

#endif  // INSERT_H
