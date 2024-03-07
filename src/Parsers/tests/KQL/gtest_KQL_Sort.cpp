#include <Parsers/tests/gtest_common.h>

#include <Parsers/Kusto/ParserKQLStatement.h>

INSTANTIATE_TEST_SUITE_P(ParserKQLQuery_Sort, ParserTest,
    ::testing::Combine(
        ::testing::Values(std::make_shared<DB::ParserKQLStatement>()),
        ::testing::ValuesIn(std::initializer_list<ParserTestCase>{
        {
            "Customers | order by FirstName",
            "SELECT *\nFROM Customers\nORDER BY FirstName DESC NULLS LAST"
        },
        {
            "Customers | order by FirstName asc",
            "SELECT *\nFROM Customers\nORDER BY FirstName ASC NULLS FIRST"
        },
        {
            "Customers | order by FirstName desc",
            "SELECT *\nFROM Customers\nORDER BY FirstName DESC NULLS LAST"
        },
        {
            "Customers | order by FirstName asc nulls first",
            "SELECT *\nFROM Customers\nORDER BY FirstName ASC NULLS FIRST"
        },
        {
            "Customers | order by FirstName asc nulls last",
            "SELECT *\nFROM Customers\nORDER BY FirstName ASC NULLS LAST"
        },
        {
            "Customers | order by FirstName desc nulls first",
            "SELECT *\nFROM Customers\nORDER BY FirstName DESC NULLS FIRST"
        },
        {
            "Customers | order by FirstName desc nulls last",
            "SELECT *\nFROM Customers\nORDER BY FirstName DESC NULLS LAST"
        },
        {
            "Customers | order by FirstName nulls first",
            "SELECT *\nFROM Customers\nORDER BY FirstName DESC NULLS FIRST"
        },
        {
            "Customers | order by FirstName nulls last",
            "SELECT *\nFROM Customers\nORDER BY FirstName DESC NULLS LAST"
        },
        {
            "Customers | order by FirstName, Age",
            "SELECT *\nFROM Customers\nORDER BY\n    FirstName DESC NULLS LAST,\n    Age DESC NULLS LAST"
        },
        {
            "Customers | order by FirstName asc, Age desc",
            "SELECT *\nFROM Customers\nORDER BY\n    FirstName ASC NULLS FIRST,\n    Age DESC NULLS LAST"
        },
        {
            "Customers | order by FirstName desc, Age asc",
            "SELECT *\nFROM Customers\nORDER BY\n    FirstName DESC NULLS LAST,\n    Age ASC NULLS FIRST"
        },
        {
            "Customers | order by FirstName asc nulls first, Age asc nulls first",
            "SELECT *\nFROM Customers\nORDER BY\n    FirstName ASC NULLS FIRST,\n    Age ASC NULLS FIRST"
        },
        {
            "Customers | order by FirstName asc nulls last, Age asc nulls last",
            "SELECT *\nFROM Customers\nORDER BY\n    FirstName ASC NULLS LAST,\n    Age ASC NULLS LAST"
        },
        {
            "Customers | order by FirstName desc nulls first, Age desc nulls first",
            "SELECT *\nFROM Customers\nORDER BY\n    FirstName DESC NULLS FIRST,\n    Age DESC NULLS FIRST"
        },
        {
            "Customers | order by FirstName desc nulls last, Age desc nulls last",
            "SELECT *\nFROM Customers\nORDER BY\n    FirstName DESC NULLS LAST,\n    Age DESC NULLS LAST"
        },
        {
            "Customers | order by FirstName nulls first, Age nulls first",
            "SELECT *\nFROM Customers\nORDER BY\n    FirstName DESC NULLS FIRST,\n    Age DESC NULLS FIRST"
        },
        {
            "Customers | order by FirstName nulls last, Age nulls last",
            "SELECT *\nFROM Customers\nORDER BY\n    FirstName DESC NULLS LAST,\n    Age DESC NULLS LAST"
        },
        {
            "Customers | order by FirstName, Age asc nulls last, LastName nulls first",
            "SELECT *\nFROM Customers\nORDER BY\n    FirstName DESC NULLS LAST,\n    Age ASC NULLS LAST,\n    LastName DESC NULLS FIRST"
        },
        {
            "Customers | order by FirstName ASC",
            "throws"
        },
        {
            "Customers | order by FirstName DESC",
            "throws"
        },
        {
            "Customers | order by FirstName nulls",
            "throws"
        },
        {
            "Customers | order by FirstName nulls middle",
            "throws"
        },
        {
            "Customers | order by FirstName asc desc",
            "throws"
        },
        {
            "Customers | order by FirstName nulls first desc",
            "throws"
        }
})));
