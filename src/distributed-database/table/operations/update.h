#ifndef UPDATE_H
#define UPDATE_H

#include "../core/table.h"
#include "table/schema.h"

/**
 * Updates record in database if satisfies condition
 * @param table
 * @param spaceMap
 * @param schema
 * @param operation
 */
extern void updateOperation(TableInfo table, TableInfo spaceMap, Schema *schema,
                            Operation operation);

/**
 * Updates record in databases if satisfies condition
 * @param tableInfo
 * @param spaceMap
 * @param queryAttributes
 * @param queryValues
 * @param cond
 * @param schema
 */
extern void updateTable(TableInfo tableInfo, TableInfo spaceMap,
                        QueryAttributes queryAttributes,
                        QueryValues queryValues, Condition cond,
                        Schema *schema);
#endif  // UPDATE_H
