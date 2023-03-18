#include <Parsers/tests/gtest_common.h>

#include <Parsers/Kusto/ParserKQLStatement.h>

INSTANTIATE_TEST_SUITE_P(ParserKQLQuery_operator, ParserTest,
    ::testing::Combine(
        ::testing::Values(std::make_shared<DB::ParserKQLStatement>()),
        ::testing::ValuesIn(std::initializer_list<ParserTestCase>{
        {
            "TableWithVariousDataTypes | project JoinDate | where JoinDate between (datetime('2020-06-30') .. datetime('2025-06-30'))",
            "SELECT JoinDate\nFROM TableWithVariousDataTypes\nWHERE kql_between(JoinDate, kql_datetime('2020-06-30'), kql_datetime('2025-06-30'))"
        },
        {
            "TableWithVariousDataTypes | project JoinDate | where JoinDate !between (datetime('2020-01-01') .. 2d)",
            "SELECT JoinDate\nFROM TableWithVariousDataTypes\nWHERE NOT kql_between(JoinDate, kql_datetime('2020-01-01'), toIntervalNanosecond(172800000000000))"
        },
        {
            "TableWithVariousDataTypes | project Age | where Age between (10 .. 12) or Age between (30 .. 50)",
            "SELECT Age\nFROM TableWithVariousDataTypes\nWHERE kql_between(Age, 10, 12) OR kql_between(Age, 30, 50)"
        }
})));
