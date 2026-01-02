#include "test/operations/createTable.h"
#include "test/operations/insertAllAttributesMultiPage.h"
#include "test/operations/insertAllAttributesSinglePage.h"
#include "test/operations/selectAttrSubset.h"
#include "test/operations/selectBetween.h"
#include "test/operations/updateStaticFields.h"
#include "test/operations/updateVarAttributes.h"
#include "test/sql/createTable.h"
#include "test/sql/selectAll.h"
#include "test/table/addSinglePageToFile.h"
#include "test/table/initialiseDatabaseFile.h"
#include "test/table/iterateRecordsMultiplePages.h"

int main() {
    testCreateTableOperation();
    testInsertAllAttributesMultiPage();
    testUpdateVarAttributes();
}