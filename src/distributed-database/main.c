#include "test/operations/selectAll.h"
#include "test/operations/selectAttrSubset.h"
#include "test/operations/selectBetween.h"
#include "test/sql/createTable.h"
#include "test/sql/deleteFromTable.h"
#include "test/sql/updateMultipleAttributes.h"

int main() {
    testSelectBetween();
}