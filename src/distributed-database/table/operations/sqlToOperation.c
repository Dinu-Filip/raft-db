#include "sqlToOperation.h"

#include <assert.h>
#include <ctype.h>
#include <string.h>

#include "hashmap.h"
#include "log.h"
#include "networking/msg.h"
#include "operation.h"

#define SELECT_ "select"

#define INSERT_START "insert"
#define INSERT_END "into"

#define UPDATE_ "update"

#define DELETE_START "delete"

#define CREATE_START "create"
#define CREATE_END "table"

#define FROM "from"
#define WHERE "where"
#define SET "set"
#define VALUES "values"

#define DELIMS " ,\n\t"

#define AND_ "and"
#define OR_ "or"
#define NOT_ "not"
#define BETWEEN_ "between"

#define INT_ "int"
#define BOOL_ "bool"
#define FLOAT_ "float"
#define STR_ "str"
#define VARSTR_ "varstr"

#define contains(str, cs)                   \
    ({                                      \
        while (*(cs++) != '\0') {           \
            if (strchr(str, *cs) != NULL) { \
                return true;                \
            }                               \
        }                                   \
        return false;                       \
    })

static bool isValidStrToken(const char *token) {
    // Ensures token is non-empty
    if (token[0] == '\0') {
        return false;
    }

    // Ensures leading character is alphabetic
    if (!isalpha(token[0])) {
        return false;
    }

    // Ensures all remaining characters are alphanumeric
    int idx = 1;
    char c;
    while ((c = token[idx++]) != '\0') {
        if (!isalnum(c)) {
            return false;
        }
    }

    return true;
}

static QueryAttributes parseSelectAttributes(char **cmd) {
    char *sql = *cmd;

    QueryAttributes attrs = malloc(sizeof(QueryAttributes));
    assert(attrs != NULL);

    attrs->numAttributes = 0;

    char *saveptr = NULL;
    char *token = strtok_r(sql, ", ", &saveptr);

    // Checks if * wildcard used for attribute list
    if (strcmp(token, "*") == 0) {
        token = strtok_r(NULL, ", ", &saveptr);

        // Ensures no other attribute names follow *
        if (strcmp(token, FROM) != 0) {
            free(attrs);
            return NULL;
        }
    }

    while (token != NULL && strcmp(token, FROM) != 0) {
        // Checks that attribute name is valid
        if (!isValidStrToken(token)) {
            free(attrs);
            return NULL;
        }

        attrs->numAttributes++;
        token = strtok_r(NULL, ", ", &saveptr);
    }

    // Checks that FROM clause follows SELECT
    if (token == NULL) {
        free(attrs);
        return NULL;
    }

    // Creates attribute list
    attrs->attributes = malloc(sizeof(AttributeName) * attrs->numAttributes);
    if (attrs->attributes == NULL) {
        free(attrs);
        return NULL;
    }

    // Sets cmd pointer to character following FROM
    *cmd = saveptr;

    unsigned idx = 0;

    // Keeps track of if prev char was terminator to track when at start of new
    // attribute
    bool prevNull = true;

    // Scans from beginning of attribute sequence, filling in the attribute list
    while (idx < attrs->numAttributes) {
        if (prevNull && *sql != '\0' && *sql != ' ' && *sql != ',') {
            attrs->attributes[idx++] = strdup(sql);
            prevNull = false;
        }
        if (*sql == '\0') {
            prevNull = true;
        }

        sql++;
    }

    return attrs;
}

static AttributeName parseAttribute(char **cmd) {
    char *saveptr;
    char *token = strtok_r(*cmd, " ;", &saveptr);

    AttributeName attrName = isValidStrToken(token) ? strdup(token) : NULL;

    return attrName;
}

static bool parseKeyword(char **cmd, char *keyword) {
    char *sql = *cmd;

    if (strlen(sql) == 0) {
        return false;
    }

    char *saveptr = NULL;

    char *token = strtok_r(sql, " ", &saveptr);

    if (strcmp(token, keyword) != 0) {
        return false;
    }

    *cmd = saveptr;
    return true;
}

static char *parseTableName(char **cmd) {
    char *sql = *cmd;

    char *saveptr = NULL;
    char *token = strtok_r(sql, " ;", &saveptr);

    if (token == NULL) {
        return NULL;
    }

    // Checks table name is valid
    if (!isValidStrToken(token)) {
        return NULL;
    }

    char *name = strdup(token);

    *cmd = saveptr;

    return name;
}

static Operand getOperand(char **cmd, char *delims) {
    char *sql = *cmd;

    // Ensures that the operand is non-empty
    if (sql[0] == '\0') {
        return NULL;
    }

    AttributeType type = -1;

    char *saveptr = NULL;
    char *token;

    // Processes initial character to determine initial type of operand
    if (sql[0] == '\"' || sql[0] == '\'') {
        // Token is a string literal

        // Checks that there exists an equivalent closing quotation mark
        if (strchr(sql + 1, sql[0]) == NULL) {
            return NULL;
        }

        char delim[] = {sql[0], '\0'};
        type = STR;
        token = strtok_r(sql + 1, delim, &saveptr);
    } else {
        token = strtok_r(sql, delims, &saveptr);
    }

    Operand op = malloc(sizeof(struct Operand));
    assert(op != NULL);

    if (type == STR) {
        op->type = STR;
        op->value.strOp = strdup(token);
        *cmd = saveptr;
        return op;
    }

    if (strcmp(token, "true") == 0) {
        op->type = BOOL;
        op->value.boolOp = true;
        *cmd = saveptr;
        return op;
    }

    if (strcmp(token, "false") == 0) {
        op->type = BOOL;
        op->value.boolOp = false;
        *cmd = saveptr;
        return op;
    }

    // Attempts to parse attribute name
    if (isValidStrToken(token)) {
        op->type = ATTR;
        op->value.strOp = strdup(token);
        *cmd = saveptr;
        return op;
    }

    char *endptr;
    op->value.intOp = strtol(sql, &endptr, 10);

    // Attempts to parse integer literal
    if (*endptr == '\0') {
        op->type = INT;
        *cmd = saveptr;
        return op;
    }

    op->value.floatOp = strtod(sql, &endptr);

    // Attempts to parse float literal
    if (*endptr == '\0') {
        op->type = FLOAT;
        *cmd = saveptr;
        return op;
    }

    free(op);
    return NULL;
}

static ConditionType getOperator(char **cmd) {
    char *sql = *cmd;

    char *saveptr = NULL;
    char *token = strtok_r(sql, " ", &saveptr);

    if (token == NULL) {
        return -1;
    }

    ConditionType type = -1;

    // Checks for logical operators
    if (strcmp(token, AND_) == 0) {
        type = AND;
    } else if (strcmp(token, OR_) == 0) {
        type = OR;
    } else if (strcmp(token, NOT_) == 0) {
        type = NOT;
    } else if (strcmp(token, BETWEEN_) == 0) {
        type = BETWEEN;
    }

    // Type has been identified so can return
    if (type != -1) {
        *cmd = saveptr;
        return type;
    }

    switch (token[0]) {
        case '=':
            *cmd = saveptr;
            return strlen(token) != 1 ? -1 : EQUALS;
        case '<':
            type = LESS_THAN;
            break;
        case '>':
            type = GREATER_THAN;
            break;
        default:
            return -1;
    }

    // Operator must not be = and must be at most 2 characters
    if (strlen(token) > 2) {
        return -1;
    }

    // Modifies type if second character is = for >= and <=
    if (token[1] == '=') {
        switch (type) {
            case LESS_THAN:
                type = LESS_EQUALS;
                break;
            case GREATER_THAN:
                type = GREATER_EQUALS;
                break;
            default:;
        }
    }

    *cmd = saveptr;
    return type;
}

static QueryTypeDescriptor getQueryType(char *cmd) {
    QueryTypeDescriptor descriptor = malloc(sizeof(struct QueryTypes));
    assert(descriptor != NULL);

    descriptor->size = -1;

    char *saveptr = NULL;
    char *token = strtok_r(cmd, " ", &saveptr);

    if (isValidStrToken(token)) {
        descriptor->name = strdup(token);
    }
    token = strtok_r(NULL, " ", &saveptr);

    if (strcmp(token, INT_) == 0) {
        descriptor->type = INT;
        return descriptor;
    }

    if (strcmp(token, FLOAT_) == 0) {
        descriptor->type = FLOAT;
        return descriptor;
    }

    if (strcmp(token, BOOL_) == 0) {
        descriptor->type = BOOL;
        return descriptor;
    }

    char *start = strtok_r(token, "(", &saveptr);
    char *rawSize = strtok_r(NULL, ")", &saveptr);

    char *end;
    int size = strtol(rawSize, &end, 10);
    if (end == rawSize) {
        free(descriptor);
        return NULL;
    }

    if (strcmp(start, STR_) == 0) {
        descriptor->type = STR;
    } else if (strcmp(start, VARSTR_) == 0) {
        descriptor->type = VARSTR;
    } else {
        free(descriptor);
        return NULL;
    }

    descriptor->size = size;
    return descriptor;
}

static bool parseOneArg(Condition condition, Operand op, ConditionType type) {
    condition->type = type;
    condition->value.oneArg.op1 = op;

    return true;
}

static bool parseTwoArg(Condition condition, char **cmd, ConditionType type,
                        Operand op1) {
    condition->type = type;
    condition->value.twoArg.op1 = op1;
    condition->value.twoArg.op2 = getOperand(cmd, "; ");

    return true;
}

static bool parseThreeArg(Condition condition, char **token, ConditionType type,
                          Operand op1) {
    condition->type = type;
    condition->value.between.op1 = op1;
    condition->value.between.op2 = getOperand(token, "; ");

    // Enforces AND as separator for values of BETWEEN
    ConditionType sepOperator = getOperator(token);
    if (sepOperator != AND) {
        free(condition->value.between.op2);
        return false;
    }

    condition->value.between.op3 = getOperand(token, "; ");
    return true;
}

static Condition parseCondition(char **cmd) {
    Condition condition = malloc(sizeof(Condition));
    assert(condition != NULL);

    // Parses the first token manually to determine if it is an operator or
    // operand
    char *saveptr;
    char *initial = strtok_r(*cmd, " ", &saveptr);

    if (initial == NULL) {
        free(condition);
        return NULL;
    }

    if (strcmp(initial, NOT_) == 0) {
        Operand op1 = getOperand(&saveptr, "; ");

        if (op1 == NULL || saveptr[0] != '\0') {
            free(condition);
            free(op1);
            return NULL;
        }

        parseOneArg(condition, op1, NOT);
        *cmd = saveptr;
        return condition;
    }

    Operand op1 = getOperand(&initial, "; ");
    *cmd = saveptr;
    ConditionType type = getOperator(cmd);

    if (type == -1) {
        free(op1);
        free(condition);
        return NULL;
    }

    if (type == BETWEEN) {
        parseThreeArg(condition, cmd, BETWEEN, op1);
    } else {
        parseTwoArg(condition, cmd, type, op1);
    }

    if (*cmd[0] != '\0' && *cmd[0] != ';') {
        free(condition);
        return NULL;
    }

    return condition;
}

static QueryAttributes createUpdateQueryAttributes(AttributeName *names,
                                                   unsigned size) {
    QueryAttributes attributes = malloc(sizeof(struct QueryAttributes));
    assert(attributes != NULL);

    attributes->attributes = malloc(sizeof(AttributeName) * size);
    assert(attributes->attributes != NULL);

    memcpy(attributes->attributes, names, size * sizeof(AttributeName));

    attributes->numAttributes = size;
    return attributes;
}

static QueryValues createUpdateQueryValues(Operand *values, unsigned size) {
    QueryValues queryValues = malloc(sizeof(struct QueryValues));
    assert(queryValues != NULL);

    queryValues->values = malloc(sizeof(Operand) * size);
    assert(queryValues->values != NULL);

    memcpy(queryValues->values, values, size * sizeof(Operand));

    queryValues->numValues = size;

    return queryValues;
}

static Operation createSelect(char *sql) {
    Operation operation = malloc(sizeof(struct Operation));
    assert(operation != NULL);

    operation->queryType = SELECT;

    // Parses the attribute list specified in the SELECT
    QueryAttributes attrs = parseSelectAttributes(&sql);

    if (attrs == NULL) {
        free(operation);
        free(attrs);
        return NULL;
    }

    operation->query.select.attributes = attrs;

    // Parses the table name in the FROM clause
    char *tableName = parseTableName(&sql);
    if (tableName == NULL) {
        free(operation);
        free(attrs);
        return NULL;
    }

    operation->tableName = tableName;

    if (parseKeyword(&sql, WHERE)) {
        // Passes the condition in the WHERE clause
        Condition condition = parseCondition(&sql);
        if (condition == NULL) {
            free(operation);
            free(attrs);
            return NULL;
        }

        operation->query.select.condition = condition;

    } else if (sql[0] != '\0') {
        free(operation);
        free(attrs);
        return NULL;
    }

    return operation;
}

static unsigned parseList(char *cmd, char *delims, void ***dest,
                          void *(*func)(char *token)) {
    unsigned size = 0;

    char *start = cmd;

    char *saveptr;
    char *token = strtok_r(cmd, delims, &saveptr);

    while (token != NULL) {
        size++;
        token = strtok_r(NULL, delims, &saveptr);
    }

    void **list = malloc(sizeof(void *) * size);
    assert(list != NULL);

    token = cmd;
    bool prevNull = true;
    int idx = 0;

    while (idx < size) {
        if (*token == '\0') {
            prevNull = true;
        } else if (prevNull && strchr(delims, *token) == NULL) {
            char *newToken = strdup(token);
            list[idx++] = func(newToken);
            free(newToken);
            prevNull = false;
        }

        token++;
    }

    *dest = list;
    return size;
}

static Operand getOperandFromList(char *token) {
    return getOperand(&token, ", ;");
}

static Operation createInsert(char *sql) {
    if (!parseKeyword(&sql, INSERT_END)) {
        return NULL;
    }

    Operation operation = malloc(sizeof(struct Operation));
    assert(operation != NULL);

    operation->queryType = INSERT;

    char *tableName = parseTableName(&sql);

    if (tableName == NULL) {
        free(operation);
        return NULL;
    }

    operation->tableName = tableName;

    while (*sql == ' ') {
        sql++;
    }

    bool hasAttributeList = *sql == '(';
    char *rest;

    if (hasAttributeList) {
        char *token = strtok_r(sql + 1, ")", &rest);
        AttributeName *names;
        unsigned numAttrs = parseList(token, ", ", (void ***)&names, strdup);
        operation->query.insert.attributes =
            createUpdateQueryAttributes(names, numAttrs);
        sql = rest;
    } else {
        operation->query.insert.attributes =
            malloc(sizeof(struct QueryAttributes));
        assert(operation->query.insert.attributes != NULL);
        operation->query.insert.attributes->numAttributes = 0;
    }

    if (!parseKeyword(&sql, VALUES)) {
        free(operation);
        return NULL;
    }

    char *saveptr;
    char *token = sql[0] != '(' ? strtok_r(sql, "(", &saveptr) : sql + 1;
    token = strtok_r(token, ")", &saveptr);

    if (token == NULL) {
        free(operation);
        return NULL;
    }

    Operand *values;
    unsigned numValues =
        parseList(token, ", ", (void ***)&values, getOperandFromList);
    operation->query.insert.values = createUpdateQueryValues(values, numValues);

    return operation;
}

static bool parseUpdateAttributeValues(Operation operation, char **cmd) {
    char *sql = *cmd;

    char *delims = ", ;";
    unsigned numAttributes = 0;

    // Approximate maximum number of attribute-value pairs
    unsigned size = strlen(sql) / 3;

    AttributeName names[size];
    Operand values[size];

    char *saveptr;
    char *token = strtok_r(sql, delims, &saveptr);

    while (token != NULL && strcmp(token, WHERE) != 0) {
        Operand op1 = getOperand(&token, delims);

        if (op1 == NULL) {
            return false;
        }

        *cmd = saveptr;
        ConditionType type = getOperator(cmd);

        if (type == -1) {
            free(op1);
            return false;
        }

        if (type != EQUALS) {
            for (int i = 0; i < numAttributes; i++) {
                free(names[i]);
                free(values[i]);
                return false;
            }
        }

        Operand op2 = getOperand(cmd, delims);

        if (op2 == NULL) {
            free(op1);
            return false;
        }

        names[numAttributes] = op1->value.strOp;
        values[numAttributes] = op2;

        free(op1);
        numAttributes++;
        token = strtok_r(*cmd, delims, &saveptr);
    }

    operation->query.update.attributes =
        createUpdateQueryAttributes(names, numAttributes);
    operation->query.update.values =
        createUpdateQueryValues(values, numAttributes);

    *cmd = saveptr;

    return operation;
}

static Operation createUpdate(char *sql) {
    Operation operation = malloc(sizeof(struct Operation));
    assert(operation != NULL);

    operation->queryType = UPDATE;

    // Attempts to parse the table name
    char *tableName = parseTableName(&sql);

    if (tableName == NULL) {
        free(operation);
        return NULL;
    }

    operation->tableName = tableName;

    // Parses set keyword
    parseKeyword(&sql, SET);

    // Parses attribute = value list
    if (!parseUpdateAttributeValues(operation, &sql)) {
        free(operation);
        free(tableName);
        return NULL;
    }

    // Attempts to parse where clause if present
    if (*sql != '\0') {
        operation->query.update.condition = parseCondition(&sql);
    }

    // End of statement should have been reached
    if (*sql != '\0') {
        free(operation);
        free(tableName);
        return NULL;
    }

    return operation;
}

static Operation createDelete(char *sql) {
    if (!parseKeyword(&sql, FROM)) {
        return NULL;
    }
    Operation operation = malloc(sizeof(struct Operation));
    assert(operation != NULL);

    operation->queryType = DELETE;

    char *tableName = parseTableName(&sql);
    if (tableName == NULL) {
        return NULL;
    }

    operation->tableName = tableName;

    if (!parseKeyword(&sql, WHERE)) {
        free(tableName);
        free(operation);
        return NULL;
    }

    Condition condition = parseCondition(&sql);
    if (condition == NULL) {
        free(tableName);
        free(operation);
        return NULL;
    }

    operation->query.delete.condition = condition;
    return operation;
}

static Operation createCreateTable(char *sql) {
    if (!parseKeyword(&sql, CREATE_END)) {
        return NULL;
    }

    Operation operation = malloc(sizeof(struct Operation));
    assert(operation != NULL);

    operation->queryType = CREATE_TABLE;

    char *tableName = parseTableName(&sql);
    if (tableName == NULL) {
        free(operation);
        return NULL;
    }

    operation->tableName = tableName;

    char *listStart = sql;

    while (*listStart != '\0' && *listStart != '(') {
        listStart++;
    }

    listStart++;

    if (*listStart == '\0') {
        free(operation);
        free(tableName);
        return NULL;
    }

    char *listEnd = sql + strlen(sql) - 1;

    while (listEnd != sql && *listEnd != ')') {
        listEnd--;
    }

    if (listEnd == listStart) {
        free(operation);
        return NULL;
    }

    *listEnd = '\0';

    QueryTypes types = malloc(sizeof(struct QueryTypes));
    assert(types != NULL);

    unsigned numTypes =
        parseList(listStart, ",", (void ***)&types->types, getQueryType);

    if (types->types == NULL) {
        free(operation);
        free(tableName);
        return NULL;
    }

    operation->query.createTable.types = types;
    operation->query.createTable.types->numTypes = numTypes;

    return operation;
}

Operation sqlToOperation(char *sql) {
    char *saveToken;
    char *token = strtok_r(sql, " ", &saveToken);

    if (strcmp(token, SELECT_) == 0) {
        return createSelect(saveToken);
    }

    if (strcmp(token, UPDATE_) == 0) {
        return createUpdate(saveToken);
    }

    if (strcmp(token, INSERT_START) == 0) {
        return createInsert(saveToken);
    }

    if (strcmp(token, DELETE_START) == 0) {
        return createDelete(saveToken);
    }

    if (strcmp(token, CREATE_START) == 0) {
        return createCreateTable(saveToken);
    }

    return NULL;
}