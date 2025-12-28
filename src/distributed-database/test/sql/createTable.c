#include "createTable.h"

#include "table/operations/sqlToOperation.h"
#include "test-library.h"

void testCreateTable() {
    char sql[] = "create table students (name varstr(50), age int, height float, student bool, code str(4));";

    Operation operation = sqlToOperation(sql);

    START_OUTER_TEST("Test create table parsing")
    ASSERT_STR_EQ(operation->tableName, "students")
    ASSERT_EQ(operation->queryType, CREATE_TABLE)
    ASSERT_EQ(operation->query.createTable.types->numTypes, 5)
    ASSERT_STR_EQ(operation->query.createTable.types->types[0]->name, "name")
    ASSERT_STR_EQ(operation->query.createTable.types->types[1]->name, "age")
    ASSERT_STR_EQ(operation->query.createTable.types->types[2]->name, "height")
    ASSERT_STR_EQ(operation->query.createTable.types->types[3]->name, "student")
    ASSERT_STR_EQ(operation->query.createTable.types->types[4]->name, "code")
    ASSERT_EQ(operation->query.createTable.types->types[0]->type, VARSTR)
    ASSERT_EQ(operation->query.createTable.types->types[0]->size, 50)
    ASSERT_EQ(operation->query.createTable.types->types[1]->type, INT)
    ASSERT_EQ(operation->query.createTable.types->types[2]->type, FLOAT)
    ASSERT_EQ(operation->query.createTable.types->types[3]->type, BOOL)
    ASSERT_EQ(operation->query.createTable.types->types[4]->type, STR)
    ASSERT_EQ(operation->query.createTable.types->types[4]->size, 4)
    FINISH_OUTER_TEST
    PRINT_SUMMARY
}