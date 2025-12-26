#ifndef UTILS_H
#define UTILS_H
#include "core/table.h"

/**
 * Enhanced display of table including headers and offsets
 * @param tableName name of table to display
 * @param tableType kind of table (RELATION, FREE_MAP, SCHEMA)
 */
extern void extendedDisplayTable(char *tableName, TableType tableType);

/**
 * Displays the records of a table
 * @param tableName name of table to display
 */
extern void displayTable(char *tableName);

/**
 * Outputs the schema of a database table
 * @param schema schema to display
 */
void outputTableSchema(Schema *schema);

#endif  // UTILS_H
