#include "test/table/addPageToFile.h"
#include "test/table/initialiseDatabaseFile.h"
#include "test/table/recordParseFixedLength.h"
#include "test/table/recordParseVarLength.h"

int main() {
    testInitialiseDatabaseFile();
    testAddPageToFile();
}