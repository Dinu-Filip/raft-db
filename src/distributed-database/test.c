#include "test.h"

#include <sys/stat.h>

#include "table/operations/operation.h"
#include "test/index/createAndOpenIndex.h"
#include "test/index/insertToRoot.h"
#include "test/index/insertToRootUnordered.h"
#include "test/index/insertWithOverflow.h"
#include "test/operations/createTable.h"
#include "test/operations/deleteMultiPage.h"
#include "test/operations/insertAllAttributesMultiPage.h"
#include "test/operations/insertAllAttributesSinglePage.h"
#include "test/operations/selectAll.h"
#include "test/operations/selectAttrSubset.h"
#include "test/operations/selectBetween.h"
#include "test/operations/updateStaticFields.h"
#include "test/operations/updateVarAttributes.h"
#include "test/sql/createTable.h"
#include "test/sql/deleteFromTable.h"
#include "test/sql/insertMultipleAttrValue.h"
#include "test/sql/insertNoAttributes.h"
#include "test/sql/insertSingleAttrValue.h"
#include "test/sql/selectAll.h"
#include "test/sql/selectMultipleAttributes.h"
#include "test/sql/selectOneArg.h"
#include "test/sql/selectOneAttribute.h"
#include "test/sql/selectThreeArg.h"
#include "test/sql/selectTwoArg.h"
#include "test/sql/updateMultipleAttributes.h"
#include "test/sql/updateMultipleAttributesWithWhere.h"
#include "test/sql/updateSingleAttribute.h"
#include "test/table/addSinglePageToFile.h"
#include "test/table/defragmentPage.h"
#include "test/table/initialiseDatabaseFile.h"
#include "test/table/insertRecordsSinglePage.h"
#include "test/table/iterateRecordsMultiplePages.h"
#include "test/table/iterateRecordsSinglePage.h"
#include "test/table/recordParseFixedLength.h"
#include "test/table/recordParseVarLength.h"

// Test fixtures write under DB_DIRECTORY, so it must exist before any test
// runs or initialiseTable's fopen fails.
static void initTestDatabasePath(void) {
    mkdir("raft-db", 0755);
    mkdir("raft-db/0", 0755);
    mkdir("raft-db/0/data", 0755);
    initDatabasePath(0);
}

int main(void) {
    initTestDatabasePath();

    testInitialiseDatabaseFile();
    testAddSinglePageToFile();
    testRecordParseFixedLength();
    testRecordParseVarLength();
    testInsertRecordsSinglePage();
    testIteratorRecordsSinglePage();
    testIteratorRecordsMultiplePages();
    testDefragmentPage();
    testDefragmentPackedPage();

    testCreateTableOperation();
    testInsertAllAttributesSinglePage();
    testInsertAllAttributesMultiPage();
    testSelectAllRecords();
    testSelectAttrSubset();
    testSelectBetween();
    testUpdateStaticFields();
    testUpdateVarAttributes();
    testDeleteMultiPage();

    testCreateTable();
    testInsertNoAttributes();
    testInsertSingleAttrValue();
    testInsertMultipleAttrValue();
    testSelectAll();
    testSelectOneAttribute();
    testSelectMultipleAttributes();
    testSelectOneArg();
    testSelectTwoArg();
    testSelectThreeArg();
    testUpdateSingleAttribute();
    testUpdateMultipleAttributes();
    testUpdateMultipleAttributesWithWhere();
    testDeleteFromTable();

    testCreateAndOpenIndex();
    testInsertToRoot();
    testInsertToRootUnordered();
    testInsertWithOverflow();

    return 0;
}
