#include "deleteMultiPage.h"

#include "table/core/recordArray.h"
#include "table/operations/operation.h"
#include "table/operations/sqlToOperation.h"
#include "test-library.h"

void testDeleteMultiPage() {
    char template[] = "delete from students where student = true;";
    executeOperation(sqlToOperation(template));

    char select1[] = "select * from students where student = true;";
    char select2[] = "select * from students;";
    QueryResult res1 = executeOperation(sqlToOperation(select1));
    QueryResult res2 = executeOperation(sqlToOperation(select2));

    START_OUTER_TEST("Test deletion of records across multiple pages")
    ASSERT_EQ(res1->records->size, 0)
    ASSERT_EQ(res2->records->size, 250)
    FINISH_OUTER_TEST
    PRINT_SUMMARY
}