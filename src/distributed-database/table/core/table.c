#include "table.h"

#include <assert.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "log.h"
#include "pages.h"
#include "record.h"
#include "recordArray.h"
#include "table/operations/sqlToOperation.h"
#include "table/operations/update.h"

#define INITIAL_NUM_PAGES 0
#define INITIAL_START_PAGE (-1)
#define INITIAL_GLOBAL_IDX 0

// Global database directory
char DB_DIRECTORY[MAX_FILE_NAME_LEN] = {'\0'};

static void initialiseHeader(FILE *headerptr) {
    LOG("Initialise header");
    fseek(headerptr, PAGE_SIZE_IDX, SEEK_SET);

    const int pageSize = _PAGE_SIZE;
    const int initialNumPages = INITIAL_NUM_PAGES;
    const int initialGlobalIdx = INITIAL_GLOBAL_IDX;

    fwrite(&pageSize, sizeof(uint8_t), PAGE_SIZE_WIDTH, headerptr);
    fwrite(&initialNumPages, sizeof(uint8_t), PAGE_ID_WIDTH, headerptr);
    fwrite(&initialGlobalIdx, sizeof(uint8_t), GLOBAL_ID_WIDTH, headerptr);

    fseek(headerptr, 0, SEEK_SET);
}

void initialiseTable(char *name) {
    char tableFile[MAX_FILE_NAME_LEN + MAX_TABLE_NAME_LEN];
    snprintf(tableFile, MAX_FILE_NAME_LEN + MAX_TABLE_NAME_LEN, "%s/%s.%s",
             DB_BASE_DIRECTORY, name, DB_EXTENSION);
    LOG("Initialise table %s\n", name);

    FILE *table = fopen(tableFile, "wb+");
    assert(table != NULL);

    initialiseHeader(table);
    fclose(table);
}

void freeTable(TableInfo tableInfo) {
    fclose(tableInfo->table);
    free(tableInfo->header);
    free(tableInfo);
}

TableInfo openTable(char *tableName) {
    char tableFile[MAX_FILE_NAME_LEN + MAX_TABLE_NAME_LEN];
    snprintf(tableFile, MAX_FILE_NAME_LEN + MAX_TABLE_NAME_LEN, "%s/%s.%s",
             DB_BASE_DIRECTORY, tableName, DB_EXTENSION);
    LOG("Open %s\n", tableFile);

    FILE *table = fopen(tableFile, "rb+");
    assert(table != NULL);

    TableInfo tableInfo = malloc(sizeof(struct TableInfo));
    assert(tableInfo != NULL);

    tableInfo->table = table;
    tableInfo->name = strdup(tableName);
    tableInfo->header = getTableHeader(table);

    return tableInfo;
}

int compareSlots(const void *slot1, const void *slot2) {
    // Compares slots in decreasing order of offset

    RecordSlot *s1 = *(RecordSlot **)slot1;
    RecordSlot *s2 = *(RecordSlot **)slot2;
    return (s1->offset < s2->offset) - (s1->offset > s2->offset);
}

TableHeader getTableHeader(FILE *table) {
    LOG("Reading header");
    TableHeader header = malloc(sizeof(struct TableHeader));
    assert(header != NULL);

    uint8_t *headerPage = getRawPage(table, _PAGE_SIZE, TABLE_HEADER_PAGE);

    memcpy(&header->pageSize, headerPage + PAGE_SIZE_IDX, PAGE_SIZE_WIDTH);
    memcpy(&header->numPages, headerPage + NUM_PAGES_IDX, PAGE_ID_WIDTH);
    memcpy(&header->globalIdx, headerPage + GLOBAL_ID_IDX, GLOBAL_ID_WIDTH);
    header->modified = false;

    free(headerPage);

    return header;
}

void freeTableHeader(TableHeader tableHeader) { free(tableHeader); }

void updateTableHeader(TableInfo tableInfo) {
    LOG("Update table header\n");

    TableHeader header = tableInfo->header;

    if (header->modified) {
        LOG("Num pages modified: %lu\n", header->numPages);
        fseek(tableInfo->table, 0, SEEK_SET);
        fwrite(&header->pageSize, sizeof(uint8_t), PAGE_SIZE_WIDTH,
               tableInfo->table);
        fwrite(&header->numPages, sizeof(uint8_t), PAGE_ID_WIDTH,
               tableInfo->table);
        fwrite(&header->globalIdx, sizeof(uint8_t), GLOBAL_ID_WIDTH,
               tableInfo->table);
        fseek(tableInfo->table, 0, SEEK_SET);
    }
}

void updateSpaceInventory(char *tableName, TableInfo spaceInventory,
                          Page page) {
    LOG("Update space inventory\n");

    int id = page->pageId;
    int freeSpace = page->header->freeSpace;

    // Update space inventory table with new free space in page
    char template[] = "update %s set FREE_SPACE = %d where PAGE_ID = %d;";
    char sql[100];

    snprintf(sql, sizeof(sql), template, spaceInventory->name, freeSpace, id);
    Schema spaceSchema = getInventorySchema();
    updateOperation(spaceInventory, NULL, &spaceSchema, sqlToOperation(sql));
}

void closeTable(TableInfo tableInfo) {
    updateTableHeader(tableInfo);
    fclose(tableInfo->table);
    free(tableInfo->header);
    free(tableInfo->name);
    free(tableInfo);
}

