#include "sqlToOperation.h"

#include <assert.h>
#include <ctype.h>
#include <string.h>

#include "operation.h"

#define SELECT_ "select"

#define INSERT_START "insert"
#define INSERT_END "into"

#define UPDATE "update"

#define DELETE_START "delete"

#define CREATE_START "create"
#define CREATE_END "table"

#define FROM "from"
#define WHERE "where"

#define DELIMS " ,\n\t"

#define AND_ "and"
#define OR_ "or"
#define NOT_ "not"
#define BETWEEN_ "between"

enum CommandState {
    START,
    INSERT_STATE,
    DELETE_STATE,
    CREATE_STATE,
    FROM_STATE,
    WHERE_STATE,
    END
};

enum ConditionState {
    START_COND,
    ONE_ARG,
    TWO_ARG,
    THREE_ARG,
    END_COND
};

enum OperandState {
    START_OP,
    STR_DOUBLE_OP,
    STR_SINGLE_OP,
    NON_STR_OP,
    END_OP
};

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

    token = strtok_r(NULL, " ;", &saveptr);

    // The next token must either be the start of a WHERE clause or the end of
    // the statement if no WHERE clause present
    if (token != NULL && strcmp(token, WHERE) != 0) {
        free(name);
        return NULL;
    }

    *cmd = saveptr;

    return name;
}

static Operand getOperand(char **cmd) {
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

        type = STR;
        token = strtok_r(sql + 1, &sql[0], &saveptr);
    } else {
        token = strtok_r(sql, " ", &saveptr);
    }

    Operand op = malloc(sizeof(struct Operand));
    assert(op != NULL);

    if (type == STR) {
        op->type = STR;
        op->value.strOp = strdup(token);
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
        case '=': return strlen(token) != 1 ? -1 : EQUALS;
        case '<': type = LESS_THAN; break;
        case '>': type = GREATER_THAN; break;
        default: return -1;
    }

    // Operator must not be = and must be at most 2 characters
    if (strlen(token) > 2) {
        return -1;
    }

    // Modifies type if second character is = for >= and <=
    if (token[1] == '=') {
        switch (type) {
            case LESS_THAN: type = LESS_EQUALS; break;
            case GREATER_THAN: type = GREATER_EQUALS; break;
            default:;
        }
    }

    *cmd = saveptr;
    return type;
}

static bool parseOneArg(Condition condition, char **cmd, ConditionType type) {
    condition->type = type;
    condition->value.oneArg.op1 = getOperand(cmd);

    return true;
}

static bool parseTwoArg(Condition condition, char **cmd, ConditionType type, Operand op1) {
    condition->type = type;
    condition->value.twoArg.op1 = op1;
    condition->value.twoArg.op2 = getOperand(cmd);

    return true;
}

static bool parseThreeArg(Condition condition, char **token, ConditionType type, Operand op1) {
    condition->type = type;
    condition->value.between.op1 = op1;
    condition->value.between.op2 = getOperand(token);

    // Enforces AND as separator for values of BETWEEN
    ConditionType sepOperator = getOperator(token);
    if (sepOperator != AND) {
        free(condition->value.between.op2);
        return false;
    }

    condition->value.between.op3 = getOperand(token);
    return true;
}

static Condition parseCondition(char **cmd) {
    Condition condition = malloc(sizeof(Condition));
    assert(condition != NULL);

    // Attempts to parse the first token as an operator in case it is a single
    // argument operator; cmd is unaffected if the token is not an operator
    ConditionType type = getOperator(cmd);

    Operand op1 = getOperand(cmd);

    if (type == NOT) {
        // The next token must be the last in the statement
        if (op1 == NULL || *cmd[0] != ';') {
            free(condition);
            free(op1);
            return NULL;
        }
        parseOneArg(condition, cmd, type);
        return condition;
    }

    type = getOperator(cmd);

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

    if (*cmd[0] != ';') {
        free(condition);
        return NULL;
    }

    return condition;
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

static Operation createInsert(char *sql) { return NULL; }

static Operation createUpdate(char *sql) { return NULL; }

static Operation createDelete(char *sql) { return NULL; }

static Operation createCreateTable(char *sql) { return NULL; }

Operation sqlToOperation(char *sql) {
    char *saveToken;
    char *token = strtok_r(sql, " ", &saveToken);

    if (strcmp(token, SELECT_) == 0) {
        return createSelect(saveToken);
    }

    return NULL;
}