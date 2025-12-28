#include "createTable.h"

#include <assert.h>
#include <limits.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>

#include "../core/field.h"
#include "../schema.h"
#include "insert.h"
#include "log.h"
#include "table/core/table.h"

static void setSchema(char *schema, char *tableName, QueryAttributes attributes,
                      const unsigned int *sizes, QueryTypes types) {
    LOG("SET SCHEMA\n");
    TableInfo schemaInfo = openTable(schema);

    // Gets data dictionary schema
    Schema *dictSchema = initDictSchema();
    // Set length of relation name to length of table name
    dictSchema->attributeSizes[RELATION_NAME_IDX] = strlen(tableName);

    // Initialises parameters for insertion into schema
    int initialIdx = 0;
    Operand nameOp = createOperand(STR, tableName);
    Operand typeOp = createOperand(INT, &initialIdx);
    Operand idxOp = createOperand(INT, &initialIdx);
    Operand sizeOp = createOperand(INT, &initialIdx);
    Operand attNameOp = createOperand(VARSTR, "");

    QueryValues values =
        createQueryValues(5, nameOp, typeOp, idxOp, sizeOp, attNameOp);

    QueryAttributes dictAttributes = createQueryAttributes(
        5, SCHEMA_RELATION_NAME, SCHEMA_ATTRIBUTE_TYPE, SCHEMA_IDX,
        SCHEMA_ATTRIBUTE_SIZE, SCHEMA_ATTRIBUTE_NAME);

    // Inserts hidden global index as first attribute
    int idx = 0;

    // Inserts non-variable length fields with index
    for (int i = 0; i < attributes->numAttributes; i++) {
        AttributeName attribute = attributes->attributes[i];
        if (types->types[i] != VARSTR) {
            idxOp->value.intOp = idx;
            typeOp->value.intOp = types->types[i];

            assert(sizes[i] <= INT_MAX);
            sizeOp->value.intOp = sizes[i];

            attNameOp->value.strOp = attribute;

            // Inserts record for attribute info into schema
            LOG("INSERTING ATTRIBUTE %s WITH TYPE %d\n", attribute,
                types->types[i]);
            insertInto(schemaInfo, NULL, dictSchema, dictAttributes, values,
                       SCHEMA);
            idx++;
        }
    }

    // Inserts variable length fields
    for (int i = 0; i < attributes->numAttributes; i++) {
        AttributeName attribute = attributes->attributes[i];
        if (types->types[i] == VARSTR) {
            typeOp->value.intOp = types->types[i];
            idxOp->value.intOp = idx;
            attNameOp->value.strOp = attribute;

            assert(sizes[i] <= INT_MAX);
            sizeOp->value.intOp = sizes[i];

            LOG("INSERTING ATTRIBUTE %s WITH TYPE %d\n", attribute,
                types->types[i]);
            insertInto(schemaInfo, NULL, dictSchema, dictAttributes, values,
                       SCHEMA);
            idx++;
        }
    }

    for (int i = 0; i < values->numValues; i++) {
        free(values->values[i]);
    }
    free(values->values);
    free(values);
    freeQueryAttributes(dictAttributes);
    free(dictSchema);
    closeTable(schemaInfo);
}

void createTable(Operation operation) {
    LOG("Execute create table\n");
    initialiseTable(operation->tableName);

    // Creates schema table
    char buffer[MAX_FILE_NAME_LEN];
    snprintf(buffer, MAX_FILE_NAME_LEN, "%s-schema", operation->tableName);
    initialiseTable(buffer);

    // Type for each attribute
    // unsigned int
    // typeSizes[operation->query.createTable.attributes->numAttributes]; Index
    // of variable length field for sizes array
    int varFieldIdx = 0;

    // for (int i = 0; i < operation->query.createTable.types->numTypes;
    //    i++) {
    //   const AttributeType type =
    //   operation->query.createTable.types->types[i];
    // if (type == VARSTR || type == STR) {
    //        typeSizes[i] =
    //            operation->query.createTable.types->sizes[varFieldIdx];
    //    varFieldIdx++;
    //    } else {
    //      typeSizes[i] = getStaticTypeWidth(type);
    //    }
    // LOG("Type size %d: %d", i, typeSizes[i]);
    // }

    // Writes the schema of the table
    // setSchema(buffer, operation->tableName,
    //          operation->query.createTable.attributes, typeSizes,
    //          operation->query.createTable.types);

    snprintf(buffer, MAX_FILE_NAME_LEN, "%s-space-inventory",
             operation->tableName);

    initialiseTable(buffer);
}
