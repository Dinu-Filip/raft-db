#include <assert.h>

#include "../lib/log.h"
#include "table.h"

void initialiseTestTable(Operation operation, char *tableName) {
    LOG("TEST TABLE CREATION\n");
    operation->queryType = CREATE_TABLE;
    operation->tableName = tableName;
    operation->query.createTable.attributes = createQueryAttributes(
        5, "ID", "NAME", "ADDRESS", "AGE", "TEST_SCORE", "PIN");
    QueryTypes types = malloc(sizeof(struct QueryTypes));
    assert(types != NULL);
    // uint8_t attributeTypes[] = {INT, VARSTR, VARSTR, INT, FLOAT, STR};
    //  operation->query.createTable.types->types = attributeTypes;
    operation->query.createTable.types->numTypes = 6;
    size_t sizes[] = {10, 8, 4};
    operation->query.createTable.types->sizes = sizes;

    executeOperation(operation);
    free(types);
    free(operation->query.createTable.attributes);
}

void testSimpleInsertSelect(Operation operation, char *tableName) {
    LOG("TEST SIMPLE INSERT SELECT");
    operation->queryType = SELECT;
    operation->query.select.attributes = createQueryAttributes(1, "NAME");
    operation->query.select.condition = malloc(sizeof(struct Condition));
    assert(operation->query.select.condition != NULL);

    operation->query.select.condition->type = EQUALS;
    operation->query.select.condition->value.twoArg.op1 = "ID";
    int idx = 1;
    operation->query.select.condition->value.twoArg.op2 =
        createOperand(INT, &idx);
    operation->tableName = tableName;

    assert(executeOperation(operation)->numRecords == 0);

    free(operation->query.select.condition->value.twoArg.op2);
    free(operation->query.select.condition);
    free(operation->query.select.attributes);

    operation->queryType = INSERT;
    operation->query.insert.attributes = createQueryAttributes(
        5, "ID", "NAME", "ADDRESS", "AGE", "TEST_SCORE", "PIN");
    int id = 1;
    float score = 96.5;
    int age = 19;
    operation->query.insert.values = createQueryValues(
        5, createOperand(INT, &id), createOperand(VARSTR, "Dinu"),
        createOperand(VARSTR, "Imperial"), createOperand(INT, &age),
        createOperand(FLOAT, &score), createOperand(STR, "1234"));

    executeOperation(operation);

    free(operation->query.insert.values->values);
    free(operation->query.insert.values);

    id = 2;
    score = 97.1;
    age = 18;
    operation->query.insert.values = createQueryValues(
        5, createOperand(INT, &id), createOperand(VARSTR, "Vlad"),
        createOperand(VARSTR, "ICL"), createOperand(INT, &age),
        createOperand(FLOAT, &score), createOperand(STR, "43A1"));

    executeOperation(operation);

    free(operation->query.insert.values->values);
    free(operation->query.insert.values);
    free(operation->query.insert.attributes->attributes);
    free(operation->query.insert.attributes);

    operation->queryType = SELECT;
    operation->query.select.attributes = createQueryAttributes(1, "NAME");
    operation->query.select.condition = malloc(sizeof(struct Condition));
    assert(operation->query.select.condition != NULL);

    operation->query.select.condition->type = EQUALS;
    operation->query.select.condition->value.twoArg.op1 = "ID";
    idx = 1;
    operation->query.select.condition->value.twoArg.op2 =
        createOperand(INT, &idx);
    operation->tableName = tableName;

    assert(executeOperation(operation)->numRecords == 1);

    operation->query.select.condition->value.twoArg.op2->value.intOp = 2;

    assert(executeOperation(operation)->numRecords == 1);
    ;
    operation->query.select.condition->value.twoArg.op2->value.intOp = 3;

    assert(executeOperation(operation)->numRecords == 0);

    operation->query.select.condition->type = LESS_THAN;
    operation->query.select.condition->value.twoArg.op1 = "AGE";
    operation->query.select.condition->value.twoArg.op2->value.intOp = 19;

    assert(executeOperation(operation)->numRecords == 2);

    free(operation->query.select.condition->value.twoArg.op2);
    free(operation->query.select.condition);
    free(operation->query.select.attributes->attributes);
    free(operation->query.select.attributes);
}

void testInsertUpdateWithPageOverflow(Operation operation, char *tableName) {
    operation->queryType = INSERT;
    operation->tableName = tableName;
    operation->query.insert.attributes =
        createQueryAttributes(3, "ID", "ITEM", "STOCK");
    int id = 0;
    int stock = 100;
    operation->query.insert.values = createQueryValues(
        3, createOperand(INT, &id), createOperand(VARSTR, "Gpu"),
        createOperand(INT, &stock));
    executeOperation(operation);
    for (int i = 0; i < 1000; i++) {
        operation->query.insert.values->values[0]->value.intOp = i;
        operation->query.insert.values->values[2]->value.intOp += 2;
        executeOperation(operation);
    }

    free(operation->query.insert.attributes->attributes);
    free(operation->query.insert.attributes);
    for (int i = 0; i < operation->query.insert.values->numValues; i++) {
        free(operation->query.insert.values->values[i]);
    }
    free(operation->query.insert.values->values);
    free(operation->query.insert.values);

    operation->queryType = UPDATE;
    operation->query.update.attributes = createQueryAttributes(1, "ITEM");
    operation->query.update.values =
        createQueryValues(1, createOperand(VARSTR, "Graphics processing unit"));
    operation->query.update.condition = malloc(sizeof(struct Condition));
    assert(operation->query.update.condition != NULL);
    operation->query.update.condition->type = EQUALS;
    operation->query.update.condition->value.twoArg.op1 = "ID";
    operation->query.update.condition->value.twoArg.op2 =
        createOperand(INT, &id);

    for (int i = 0; i < 500; i += 2) {
        operation->query.update.condition->value.twoArg.op2->value.intOp = i;
        executeOperation(operation);
    }
    for (int i = 0; i < operation->query.update.values->numValues; i++) {
        free(operation->query.update.values->values[i]);
    }
    free(operation->query.update.values);
    free(operation->query.update.condition);
    free(operation->query.update.attributes->attributes);
    free(operation->query.update.attributes);

    operation->queryType = INSERT;
    operation->tableName = tableName;
    operation->query.insert.attributes =
        createQueryAttributes(3, "ID", "ITEM", "STOCK");
    id = 0;
    stock = 50;
    operation->query.insert.values = createQueryValues(
        3, createOperand(INT, &id), createOperand(VARSTR, "Rainbow LED"),
        createOperand(INT, &stock));
    executeOperation(operation);
    for (int i = 0; i < 250; i++) {
        operation->query.insert.values->values[0]->value.intOp = i;
        operation->query.insert.values->values[2]->value.intOp++;
        executeOperation(operation);
    }

    free(operation->query.insert.attributes->attributes);
    free(operation->query.insert.attributes);
    for (int i = 0; i < operation->query.insert.values->numValues; i++) {
        free(operation->query.insert.values->values[i]);
    }
    free(operation->query.insert.values->values);
    free(operation->query.insert.values);
}

void testInsertDeleteWithPageOverflow(Operation operation, char *tableName) {
    operation->queryType = INSERT;
    operation->tableName = tableName;
    operation->query.insert.attributes =
        createQueryAttributes(3, "ID", "ITEM", "STOCK");
    int id = 0;
    int stock = 100;
    operation->query.insert.values = createQueryValues(
        3, createOperand(INT, &id), createOperand(VARSTR, "Gpu"),
        createOperand(INT, &stock));
    executeOperation(operation);
    for (int i = 0; i < 1000; i++) {
        operation->query.insert.values->values[0]->value.intOp = i;
        operation->query.insert.values->values[2]->value.intOp += 2;
        executeOperation(operation);
    }

    free(operation->query.insert.attributes->attributes);
    free(operation->query.insert.attributes);
    for (int i = 0; i < operation->query.insert.values->numValues; i++) {
        free(operation->query.insert.values->values[i]);
    }
    free(operation->query.insert.values->values);
    free(operation->query.insert.values);

    operation->queryType = DELETE;
    operation->query.delete.condition = malloc(sizeof(struct Condition));
    assert(operation->query.delete.condition != NULL);

    operation->query.delete.condition->type = EQUALS;
    operation->query.delete.condition->value.twoArg.op1 = "ID";
    operation->query.delete.condition->value.twoArg.op2 =
        createOperand(INT, &id);
    for (int i = 0; i < 500; i += 2) {
        operation->query.update.condition->value.twoArg.op2->value.intOp = i;
        executeOperation(operation);
    }

    free(operation->query.delete.condition->value.twoArg.op2);
    free(operation->query.delete.condition);

    operation->queryType = INSERT;
    operation->tableName = tableName;
    operation->query.insert.attributes =
        createQueryAttributes(3, "ID", "ITEM", "STOCK");
    id = 0;
    stock = 50;
    operation->query.insert.values = createQueryValues(
        3, createOperand(INT, &id), createOperand(VARSTR, "Rainbow LED"),
        createOperand(INT, &stock));
    executeOperation(operation);
    for (int i = 0; i < 250; i++) {
        operation->query.insert.values->values[0]->value.intOp = i;
        operation->query.insert.values->values[2]->value.intOp++;
        executeOperation(operation);
    }

    free(operation->query.insert.attributes->attributes);
    free(operation->query.insert.attributes);
    for (int i = 0; i < operation->query.insert.values->numValues; i++) {
        free(operation->query.insert.values->values[i]);
    }
    free(operation->query.insert.values->values);
    free(operation->query.insert.values);
}
