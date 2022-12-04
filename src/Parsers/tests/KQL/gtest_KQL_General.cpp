#include <Parsers/tests/gtest_common.h>

#include <Parsers/Kusto/ParserKQLQuery.h>

INSTANTIATE_TEST_SUITE_P(ParserKQLQuery_General, ParserTest,
    ::testing::Combine(
        ::testing::Values(std::make_shared<DB::ParserKQLQuery>()),
        ::testing::ValuesIn(std::initializer_list<ParserTestCase>{
        {
            "print t = case(5 <= 10, 'A', 12 <= 20, 'B', 22 <= 30, 'C', 'D')",
            "SELECT multiIf(5 <= 10, 'A', 12 <= 20, 'B', 22 <= 30, 'C', 'D') AS t"
        },
        {
            "Customers | extend t = case(Age <= 10, 'A', Age <= 20, 'B', Age <= 30, 'C', 'D')",
            "SELECT\n    * EXCEPT t,\n    multiIf(Age <= 10, 'A', Age <= 20, 'B', Age <= 30, 'C', 'D') AS t\nFROM Customers"
        },
        {
            "Customers | extend t = iff(Age < 20, 'little', 'big')",
            "SELECT\n    * EXCEPT t,\n    If(Age < 20, 'little', 'big') AS t\nFROM Customers"
        },
        {
            "Customers | extend t = iif(Age < 20, 'little', 'big')",
            "SELECT\n    * EXCEPT t,\n    If(Age < 20, 'little', 'big') AS t\nFROM Customers"
        }
})));
