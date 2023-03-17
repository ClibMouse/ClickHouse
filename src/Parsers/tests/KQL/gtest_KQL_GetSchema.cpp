#include <Parsers/tests/gtest_common.h>

#include <Parsers/Kusto/ParserKQLStatement.h>

INSTANTIATE_TEST_SUITE_P(ParserKQLQuery_GetSchema, ParserTest,
    ::testing::Combine(
        ::testing::Values(std::make_shared<DB::ParserKQLStatement>()),
        ::testing::ValuesIn(std::initializer_list<ParserTestCase>{
        {
            "Customers | getschema",
            "SELECT *\nFROM getschema(\n    SELECT *\n    FROM Customers\n)"
        },
        {
            "Customers | getschema | getschema",
            "SELECT *\nFROM getschema(\n    SELECT *\n    FROM\n    (\n        SELECT *\n        FROM getschema(\n            SELECT *\n            FROM Customers\n        )\n    )\n)"
        },
        {
            "print x = 'asd' | extend y = strlen(x) | getschema",
            "SELECT *\nFROM getschema(\n    SELECT *\n    FROM\n    (\n        SELECT\n            * EXCEPT y,\n            lengthUTF8(x) AS y\n        FROM\n        (\n            SELECT 'asd' AS x\n        )\n    )\n)"
        }
})));
