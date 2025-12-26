#ifndef CREATE_TABLE_H
#define CREATE_TABLE_H

#include "table/operations/operation.h"

/**
 * Creates new table with schema and space inventory
 * @param operation operation parameters
 */
extern void createTable(Operation operation);

#endif  // CREATE_TABLE_H
