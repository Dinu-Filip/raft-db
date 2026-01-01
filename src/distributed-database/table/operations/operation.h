#ifndef OPERATION_H
#define OPERATION_H

#include <stdbool.h>
#include <stdint.h>
#include <stdlib.h>

#include "table/core/table.h"

// uint8_t is used to represent enums within the struct to make parsing and
// encoding easier
typedef enum { INT = 0, STR, VARSTR, FLOAT, BOOL, ATTR } AttributeType;
typedef enum { SELECT, INSERT, UPDATE, DELETE, CREATE_TABLE } QueryType;
typedef enum {
    EQUALS,
    LESS_THAN,
    GREATER_THAN,
    LESS_EQUALS,
    GREATER_EQUALS,
    BETWEEN,
    AND,
    OR,
    NOT
} ConditionType;

typedef char *AttributeName;
typedef struct QueryResult *QueryResult;

typedef struct Operand *Operand;
struct Operand {
    AttributeType type;
    union {
        int32_t intOp;
        char *strOp;
        float floatOp;
        bool boolOp;
    } value;
};

typedef struct QueryValues *QueryValues;
struct QueryValues {
    Operand *values;
    uint16_t numValues;
};

typedef struct QueryAttributes *QueryAttributes;
struct QueryAttributes {
    AttributeName *attributes;
    uint16_t numAttributes;
};

typedef struct QueryTypeDescriptor *QueryTypeDescriptor;
struct QueryTypeDescriptor {
    AttributeName name;
    AttributeType type;
    unsigned size;
};

typedef struct QueryTypes *QueryTypes;
struct QueryTypes {
    QueryTypeDescriptor *types;
    unsigned numTypes;
};

typedef struct Condition *Condition;
struct Condition {
    ConditionType type;
    union {
        struct {
            Operand op1;
        } oneArg;
        struct {
            Operand op1;
            Operand op2;
        } twoArg;
        struct {
            Operand op1;
            Operand op2;
            Operand op3;
        } between;
    } value;
};

typedef struct Operation *Operation;
struct Operation {
    char *tableName;
    QueryType queryType;
    union {
        struct {
            QueryAttributes attributes;
            Condition condition;
        } select;
        struct {
            QueryAttributes attributes;
            QueryValues values;
        } insert;
        struct {
            QueryAttributes attributes;
            QueryValues values;
            Condition condition;
        } update;
        struct {
            Condition condition;
        } delete;
        struct {
            QueryTypes types;
        } createTable;
    } query;
};

/**
 * Initialise the global path to the database directory
 * @param nodeId id of node on which database runs
 */
extern void initDatabasePath(size_t nodeId);

extern QueryResult executeQualifiedOperation(Operation operation, TableType tableType);

/**
 * Executes operation, returning result of query for SELECT or NULL otherwise
 * @param operation
 */
extern QueryResult executeOperation(Operation operation);

/**
 * Determines whether operation is read (select) or write (any other operation)
 * @param operation
 */
extern bool isWriteOperation(Operation operation);

#endif  // OPERATION_H
