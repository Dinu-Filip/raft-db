#ifndef CONDITIONS_H
#define CONDITIONS_H

#include <stdbool.h>

#include "table.h"
#include "table/operation.h"

/**
 * Evaluate a condition on the given record
 * @param record the record to evaluate on
 * @param condition the condition to evaluate
 */
extern bool evaluate(Record record, Condition condition);

#endif  // CONDITIONS_H
