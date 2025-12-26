#include "test/table/initialiseDatabaseFile.h"
#include "test/table/recordParseFixedLength.h"
#include "test/table/recordParseVarLength.h"

int main() {
    testRecordParseFixedLength();
    testRecordParseVarLength();
    testInitialiseDatabaseFile();
}