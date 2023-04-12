#include <Parsers/tests/gtest_common.h>

#include <Parsers/Kusto/ParserKQLStatement.h>

INSTANTIATE_TEST_SUITE_P(ParserKQLQuery_ProjectRename, ParserTest,
    ::testing::Combine(
        ::testing::Values(std::make_shared<DB::ParserKQLStatement>()),
        ::testing::ValuesIn(std::initializer_list<ParserTestCase>{
        {
            "Customers | project-rename FN=FirstName",
            "SELECT\n    * EXCEPT FirstName,\n    FirstName AS FN\nFROM Customers"
        },
        {
            "print FirstName='FN', LastName='LN' | project-rename FN=FirstName, LN=LastName",
            "SELECT\n    * EXCEPT (FirstName, LastName),\n    FirstName AS FN,\n    LastName AS LN\nFROM\n(\n    SELECT\n        'FN' AS FirstName,\n        'LN' AS LastName\n)"
        },
        {
            "print FirstName='FN', LastName='LN' | project-rename FN=FirstName, LN=LastName, LastName",
            "SELECT\n    * EXCEPT (FirstName, LastName),\n    FirstName AS FN,\n    LastName AS LN\nFROM\n(\n    SELECT\n        'FN' AS FirstName,\n        'LN' AS LastName\n)"
        }
})));
