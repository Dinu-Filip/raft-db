#include "sqlToOperation.h"

#include <string.h>
#include "operation.h"

#define SELECT "select"

#define INSERT_START "insert"
#define INSERT_END "into"

#define UPDATE "update"

#define DELETE_START "delete"
#define DELETE_END "from"

#define CREATE_START "create"
#define CREATE_END "table"

enum CommandState {
    START,
    INSERT_STATE,
    DELETE_STATE,
    CREATE_STATE
};

static Operation createSelect(char *sql) { return; }

static Operation createInsert(char *sql) { return; }

static Operation createUpdate(char *sql) { return; }

static Operation createDelete(char *sql) { return; }

static Operation createCreateTable(char *sql) { return; }

Operation sqlToOperation(char *sql) {
    char *saveToken;

    char *token = strtok_r(sql, " ", &saveToken);

    enum CommandState state = START;

    while (token != NULL) {
        if (state == START) {
            if (strcmp(token, SELECT) == 0) { return createSelect(saveToken); }
            if (strcmp(token, UPDATE) == 0) { return createUpdate(saveToken); }

            if (strcmp(token, INSERT_START) == 0) { state = INSERT_STATE; }
            else if (strcmp(token, DELETE_START) == 0) { state = DELETE_STATE; }
            else if (strcmp(token, CREATE_START) == 0) { state = CREATE_STATE; }
            else { return NULL; }
        } else {
            if (state == INSERT_STATE && strcmp(token, INSERT_END) == 0) { return createInsert(saveToken); }
            if (state == DELETE_STATE && strcmp(token, DELETE_END) == 0) { return createDelete(saveToken); }
            if (state == CREATE_STATE && strcmp(token, CREATE_END) == 0) { return createCreateTable(saveToken); }

            return NULL;
        }
        strtok_r(token, " ", &saveToken);
    }
    return NULL;
}