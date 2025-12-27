#include "test/sql/selectMultipleAttributes.h"
#include "test/sql/selectOneArg.h"
#include "test/sql/selectOneAttribute.h"
#include "test/sql/selectTwoArg.h"

int main() {
    testSelectMultipleAttributes();
    testSelectOneArg();
    testSelectOneAttribute();
    testSelectTwoArg();
}