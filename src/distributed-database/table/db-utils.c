#include "db-utils.h"

#include <string.h>

#include "pages.h"
#include "table.h"
#include "utils.h"

#define MAX(x, y) (x > y ? x : y)

void displayTable(char *tableName) {
    TableInfo relationInfo = openTable(tableName);

    // Opens table
    char buffer[MAX_FILE_NAME_LEN];
    snprintf(buffer, MAX_FILE_NAME_LEN, "%s-schema", tableName);
    TableInfo schemaInfo = openTable(buffer);

    Schema *schema = getSchema(schemaInfo, tableName);

    RecordIterator iterator;
    initialiseRecordIterator(&iterator);

    Record record = iterateRecords(relationInfo, schema, &iterator, true);

    outputTableSchema(schema);
    while (record != NULL) {
        printf("|");

        // Skips global idx
        for (int i = 1; i < record->numValues; i++) {
            Field field = record->fields[i];
            AttributeName attribute = schema->attributes[i - 1];
            printf(" ");
            outputField(field, MAX((int)strlen(attribute),
                                   schema->attributeSizes[i - 1]));
            printf(" |");
        }

        printf("\n");
        freeRecord(record);
        record = iterateRecords(relationInfo, schema, &iterator, true);
    }

    closeTable(relationInfo);
}

void outputTableSchema(Schema *schema) {
    // Displays the attributes of the table as columns
    printf("|");

    for (int i = 0; i < schema->numAttributes; i++) {
        AttributeName attribute = schema->attributes[i];
        printf(" ");
        printf("%-*s", MAX(schema->attributeSizes[i], (int)strlen(attribute)),
               schema->attributes[i]);
        printf(" |");
    }
    printf("\n");
}

static void outputPageHeader(Page page) {
    printf("-----------PAGE %hu HEADER-----------\n", page->pageId);
    printf("Number of records: %d\n", page->header->numRecords);
    printf("Offset to record start: %d\n", page->header->recordStart);
    printf("Amount of free space: %d\n", page->header->freeSpace);
    printf("\n");
    printf("Record slots (pos: (offset, size)): \n");

    int i = 0;
    RecordSlot slot;
    do {
        slot = page->header->recordSlots[i++];
        long diff = slot.pos - page->ptr;
        printf("%ld: ", diff);
        if (slot.size == 0) {
            printf("(empty)\n");
        } else if (slot.size == PAGE_TAIL) {
            printf("(end)\n");
        } else {
            printf("(%d, %d)\n", slot.offset, slot.size);
        }
    } while (slot.size != PAGE_TAIL);
}

static void outputDisplayField(Field field, unsigned int rightPadding) {
    switch (field.type) {
        case INT:
            printf("%-*d", rightPadding, field.intValue);
            break;
        case FLOAT:
            printf("%-*f", rightPadding, field.floatValue);
            break;
        case BOOL:
            printf("%-*d", rightPadding, field.boolValue);
            break;
        case STR:
        case VARSTR:
            printf("%-*s", rightPadding, field.stringValue);
            break;
        default:
            break;
    }
}

void extendedDisplayTable(char *tableName, TableType tableType) {
    TableInfo relationInfo;
    TableInfo schemaInfo;
    Schema *schema;
    if (tableType == RELATION) {
        relationInfo = openTable(tableName);

        char buffer[MAX_FILE_NAME_LEN];
        snprintf(buffer, MAX_FILE_NAME_LEN, "%s-schema", tableName);
        schemaInfo = openTable(buffer);

        schema = getSchema(schemaInfo, tableName);
    } else if (tableType == SCHEMA) {
        char buffer[MAX_FILE_NAME_LEN];
        snprintf(buffer, MAX_FILE_NAME_LEN, "%s-schema", tableName);
        relationInfo = openTable(buffer);

        schema = initDictSchema();
        schema->attributeSizes[RELATION_NAME_IDX] = strlen(tableName);
    } else {
        char buffer[MAX_FILE_NAME_LEN];
        snprintf(buffer, MAX_FILE_NAME_LEN, "%s-space-inventory", tableName);
        relationInfo = openTable(buffer);

        schema = initSpaceInfoSchema(tableName);
    }

    printf("-----------TABLE HEADER-----------\n");
    printf("Page size: %lu\n", relationInfo->header->pageSize);
    printf("Number of pages: %lu\n", relationInfo->header->numPages);
    printf("Start page: %d\n", relationInfo->header->startPage);

    RecordIterator iterator;
    initialiseRecordIterator(&iterator);

    Record record = iterateRecords(relationInfo, schema, &iterator, true);

    if (record == NULL) {
        printf("%s has no records\n", tableName);
        return;
    }

    Page page = iterator.page;
    outputPageHeader(page);
    printf("-----------PAGE %hu RECORDS-----------\n", page->pageId);

    outputTableSchema(schema);
    while (record != NULL) {
        // Skips global idx
        printf("|");
        for (int i = 1; i < record->numValues; i++) {
            Field field = record->fields[i];
            AttributeName attribute = schema->attributes[i - 1];
            printf(" ");
            outputDisplayField(field, MAX((int)strlen(attribute),
                                          schema->attributeSizes[i - 1]));
            printf(" |");
        }
        freeRecord(record);
        printf("\n");
        record = iterateRecords(relationInfo, schema, &iterator, true);
        if (record != NULL && iterator.page->pageId != page->pageId) {
            page = iterator.page;
            outputPageHeader(page);
            printf("-----------PAGE %hu RECORDS-----------\n", page->pageId);
            outputTableSchema(schema);
        }
    }
    freeRecordIterator(iterator);
    closeTable(relationInfo);
    if (tableType == RELATION) {
        freeSchema(schema);
        closeTable(schemaInfo);
    } else {
        free(schema);
    }
}
