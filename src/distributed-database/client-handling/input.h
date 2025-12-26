#ifndef CLIENT_HANDLING_INPUT_H
#define CLIENT_HANDLING_INPUT_H

#include "table/operations/operation.h"

extern Operation parseOperationJson(const char *jsonString);

extern char *queryResultStringify(QueryResult queryResult);

#endif  // CLIENT_HANDLING_INPUT_H
