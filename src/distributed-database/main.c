#include "test/table/addSinglePageToFile.h"
#include "test/table/initialiseDatabaseFile.h"
#include "test/table/insertRecordsSinglePage.h"
#include "test/table/recordParseFixedLength.h"
#include "test/table/recordParseVarLength.h"

int main() {
    testInsertRecordsSinglePage();
}