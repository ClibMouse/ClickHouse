#include <Parsers/tests/gtest_common.h>

#include <Parsers/Kusto/ParserKQLStatement.h>

INSTANTIATE_TEST_SUITE_P(ParserKQLQuery_Count, ParserTest,
    ::testing::Combine(
        ::testing::Values(std::make_shared<DB::ParserKQLStatement>()),
        ::testing::ValuesIn(std::initializer_list<ParserTestCase>{
        {
            "Customers | count",
            "SELECT count() AS Count\nFROM Customers"
        },
        {
            "Customers | where Age< 30 | count",
            "SELECT count() AS Count\nFROM\n(\n    SELECT *\n    FROM Customers\n    WHERE Age < 30\n)"
        },
        {
            "Customers | where Age< 30 | limit 2| count",
            "SELECT count() AS Count\nFROM\n(\n    SELECT *\n    FROM Customers\n    WHERE Age < 30\n    LIMIT 2\n)"
        },
        {
            "Customers | where Age< 30 | limit 2 | count | project Count",
            "SELECT Count\nFROM\n(\n    SELECT count() AS Count\n    FROM\n    (\n        SELECT *\n        FROM Customers\n        WHERE Age < 30\n        LIMIT 2\n    )\n)"
        },
        {
            "Customers|project FirstName|where FirstName != 'Peter'|sort by FirstName asc nulls first|count",
            "SELECT count() AS Count\nFROM\n(\n    SELECT FirstName\n    FROM Customers\n    WHERE FirstName != 'Peter'\n    ORDER BY FirstName ASC NULLS FIRST\n)"
        }
})));
