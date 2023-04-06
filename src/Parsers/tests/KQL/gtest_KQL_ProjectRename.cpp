#include <Parsers/tests/gtest_common.h>

#include <Parsers/Kusto/ParserKQLStatement.h>

INSTANTIATE_TEST_SUITE_P(ParserKQLQuery_ProjectRename, ParserTest,
    ::testing::Combine(
        ::testing::Values(std::make_shared<DB::ParserKQLStatement>()),
        ::testing::ValuesIn(std::initializer_list<ParserTestCase>{
        {
            "Customers | project-rename FN=FirstName",
            "SELECT * EXCEPT FirstName, FirstName AS FN FROM Customers"
        },
        {
            "print FirstName='FN', LastName='LN' | project-rename FN=FirstName, LN=LastName",
            "SELECT * EXCEPT (FirstName, LastName), FirstName AS FN, LastName AS LN FROM (SELECT 'FN' AS FirstName, 'LN' AS LastName)"
        }
})));
