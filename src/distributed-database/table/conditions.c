#include "conditions.h"

#include <string.h>

#include "table/table.h"

#define EVALUATE_COND(field, op, value)                                \
    switch (value->type) {                                             \
        case STR:                                                      \
        case VARSTR:                                                   \
            return strcmp(field.stringValue, value->value.strOp) op 0; \
        case INT:                                                      \
            return field.intValue op value->value.intOp;               \
        case FLOAT:                                                    \
            return field.floatValue op value->value.floatOp;           \
        case BOOL:                                                     \
            return field.boolValue op value->value.boolOp;             \
        default:                                                       \
            return false;                                              \
    }

static bool evaluateBetween(Field field, Operand value1, Operand value2) {
    switch (field.type) {
        case INT:
            return value1->value.intOp <= field.intValue &&
                   field.intValue <= value2->value.intOp;
        case FLOAT:
            return value1->value.floatOp <= field.floatValue &&
                   field.floatValue <= value2->value.floatOp;
        default:
            return false;
    }
}

static bool evaluateTwoArg(ConditionType type, Field field, Operand value) {
    switch (type) {
        case EQUALS:
            EVALUATE_COND(field, ==, value);
        case LESS_THAN:
            EVALUATE_COND(field, <, value);
        case GREATER_THAN:
            EVALUATE_COND(field, >, value);
        case LESS_EQUALS:
            EVALUATE_COND(field, <=, value);
        case GREATER_EQUALS:
            EVALUATE_COND(field, >=, value);
        default:
            return false;
    }
}

bool evaluate(Record record, Condition condition) {
    Attribute attribute;
    if (condition->type == BETWEEN) {
        attribute = condition->value.between.op1;
    } else {
        attribute = condition->value.twoArg.op1;
    }

    for (int i = 0; i < record->numValues; i++) {
        Field field = record->fields[i];

        if (strcmp(field.attribute, attribute) == 0) {
            if (condition->type == BETWEEN) {
                Operand op2 = condition->value.between.op2;
                Operand op3 = condition->value.between.op3;
                return evaluateBetween(field, op2, op3);
            }
            Operand op = condition->value.twoArg.op2;
            return evaluateTwoArg(condition->type, field, op);
        }
    }

    return false;
}
