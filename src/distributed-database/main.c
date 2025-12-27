#include "test/table/addSinglePageToFile.h"
#include "test/table/defragmentPage.h"
#include "test/table/initialiseDatabaseFile.h"
#include "test/table/insertRecordsSinglePage.h"
#include "test/table/iterateRecordsMultiplePages.h"
#include "test/table/iterateRecordsSinglePage.h"
#include "test/table/recordParseFixedLength.h"
#include "test/table/recordParseVarLength.h"

int main() {
    testDefragmentPage();
}