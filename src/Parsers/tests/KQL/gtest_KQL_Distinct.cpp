#include <Parsers/tests/gtest_common.h>

#include <Parsers/Kusto/ParserKQLStatement.h>

INSTANTIATE_TEST_SUITE_P(ParserKQLQuery_Distinct, ParserKQLTest,
    ::testing::Combine(
        ::testing::Values(std::make_shared<DB::ParserKQLStatement>()),
        ::testing::ValuesIn(std::initializer_list<ParserTestCase>{
        {
            "Customers | distinct *",
            "SELECT DISTINCT *\nFROM Customers"
        },
        {
            "Customers | distinct Occupation",
            "SELECT DISTINCT Occupation\nFROM Customers"
        },
        {
            "Customers | distinct Occupation, Education",
            "SELECT DISTINCT\n    Occupation,\n    Education\nFROM Customers"
        },
        {
            "Customers |where Age <30| distinct Occupation, Education",
            "SELECT DISTINCT\n    Occupation,\n    Education\nFROM\n(\n    SELECT *\n    FROM Customers\n    WHERE Age < 30\n)"
        },
        {
            "Customers |where Age <30 | order by Age| distinct Occupation, Education",
            "SELECT DISTINCT\n    Occupation,\n    Education\nFROM\n(\n    SELECT *\n    FROM Customers\n    WHERE Age < 30\n    ORDER BY Age DESC NULLS LAST\n)"
        },
        {
            "Customers | project a = (Age % 10) | distinct a;",
            "SELECT DISTINCT a\nFROM\n(\n    SELECT Age % 10 AS a\n    FROM Customers\n)"
        }
})));
