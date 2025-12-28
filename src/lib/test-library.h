#include <stdio.h>
#include <string.h>

#include "../distributed-database/table/operations/operation.h"

static int numTests = 0;
static int numPassed = 0;
static int outputIndent = 0;

#define INDENT(s)                                       \
    for (int i = 0; i < outputIndent; i++) printf(" "); \
    printf(s);                                          \
    printf("\n");

#define BAR INDENT("--------------------------------")

#define START_OUTER_TEST(title) \
    BAR INDENT(title)           \
    BAR outputIndent += 4;

#define FINISH_OUTER_TEST \
    printf("\n");         \
    outputIndent -= 4;

#define TEST(cond)                  \
    {                               \
        numTests++;                 \
        if (cond) {                 \
            numPassed++;            \
            INDENT("PASS: " #cond); \
        } else {                    \
            INDENT("FAIL: " #cond); \
        }                           \
    }

#define ASSERT_EQ(a, b) TEST(a == b)
#define ASSERT_STR_EQ(a, b) TEST(strcmp(a, b) == 0)
#define ASSERT_LIST_EQ(l1, n1, l2, n2) \
    {                                  \
        ASSERT_EQ(n1, n2);             \
        for (int i = 0; i < n1; i++) { \
            ASSERT_EQ(l1[i], l2[i]);   \
        }                              \
    }
#define ASSERT_LIST_STR_EQ(l1, n1, l2, n2) \
    {                                      \
        ASSERT_EQ(n1, n2);                 \
        for (int i = 0; i < n1; i++) {     \
            ASSERT_STR_EQ(l1[i], l2[i]);   \
        }                                  \
    }
#define ASSERT_NEQ(a, b) TEST(a != b)
#define ASSERT_F(a, b, f) TEST(f(a, b))
#define FAIL TEST(false)
#define PRINT_SUMMARY printf("Passed %d / %d\n", numPassed, numTests);
