#ifndef OPERATION_H
#define OPERATION_H

#include <stdbool.h>
#include <stdint.h>
#include <stdlib.h>

// uint8_t is used to represent enums within the struct to make parsing and
// encoding easier
typedef enum { INT = 0, STR, VARSTR, FLOAT, BOOL } AttributeType;
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
    uint8_t type;
    union {
        int intOp;
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

typedef struct QueryTypes *QueryTypes;
struct QueryTypes {
    AttributeType *types;
    uint16_t numTypes;
    size_t *sizes;
    uint16_t numSizes;
};

typedef struct Condition *Condition;
struct Condition {
    uint8_t type;
    union {
        struct {
            AttributeName op1;
        } oneArg;
        struct {
            AttributeName op1;
            Operand op2;
        } twoArg;
        struct {
            AttributeName op1;
            Operand op2;
            Operand op3;
        } between;
    } value;
};

typedef struct Operation *Operation;
struct Operation {
    char *tableName;
    uint8_t queryType;
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
            QueryAttributes attributes;
            QueryTypes types;
        } createTable;
    } query;
};

/**
 * Initialise the global path to the database directory
 * @param nodeId id of node on which database runs
 */
extern void initDatabasePath(size_t nodeId);

/**
 * Executes operation, returning result of query for SELECT or NULL otherwise
 * @param operation
 */
extern QueryResult executeOperation(Operation operation);

/**
 * Creates Operation for two arguments comparisons in conditions
 */
extern Operation createSelectTwoArgOperation();

/**
 * Frees select operation with two argument condition
 * @param operation
 */
extern void freeSelectTwoArgOperation(Operation operation);

/**
 * Creates insert Operation
 * @return insert Operation
 */
extern Operation createInsertOperation();

/**
 * Frees insert operation
 * @param operation
 */
extern void freeInsertOperationTest(Operation operation);

/**
 * Creates QueryAttributes with provided attributes
 * @param numAttributes
 * @param ... attributes to add
 * @return QueryAttributes struct with added attributes
 */
extern QueryAttributes createQueryAttributes(size_t numAttributes, ...);

/**
 * Frees query attributes
 * @param attributes
 */
extern void freeQueryAttributes(QueryAttributes attributes);

/**
 * Creates Operand with given type and value
 * @param type type of operand
 * @param value value to insert in Operand
 */
extern Operand createOperand(AttributeType type, void *value);

/**
 * Creates QueryValues with provided Operands
 * @param numValues number of values in QueryValues
 * @param ... Operand fields
 */
extern QueryValues createQueryValues(size_t numValues, ...);

extern void freeQueryValues(QueryValues queryValues);

/**
 * Determines whether operation is read (select) or write (any other operation)
 * @param operation
 */
extern bool isWriteOperation(Operation operation);

#endif  // OPERATION_H
