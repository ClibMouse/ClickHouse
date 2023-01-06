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
        },
        {
            "print res = bin_at(6.5, 2.5, 7)",
            "SELECT kql_bin_at(6.5, 2.5, 7) AS res"
        },
        {
            "print res = bin_at(1h, 1d, 12h)",
            "SELECT kql_bin_at(toIntervalNanosecond(3600000000000), toIntervalNanosecond(86400000000000), toIntervalNanosecond(43200000000000)) AS res"
        },
        {
            "print res = bin_at(datetime(2017-05-15 10:20:00.0), 1d, datetime(1970-01-01 12:00:00.0))",
            "SELECT kql_bin_at(kql_datetime('2017-05-15 10:20:00.0'), toIntervalNanosecond(86400000000000), kql_datetime('1970-01-01 12:00:00.0')) AS res"
        },
        {
            "print bin(4.5, 1)",
            "SELECT kql_bin(4.5, 1)"
        },
        {
            "print bin(4.5, -1)",
            "SELECT kql_bin(4.5, -1)"
        },
        {
            "print bin(time(16d), 7d)",
            "SELECT kql_bin(toIntervalNanosecond(1382400000000000), toIntervalNanosecond(604800000000000))"
        },
        {
            "print bin(datetime(1970-05-11 13:45:07), 1d)",
            "SELECT kql_bin(kql_datetime('1970-05-11 13:45:07'), toIntervalNanosecond(86400000000000))"
        },
        {
            "print bin(datetime(1970-05-11 13:45:07.456345672), 1ms)",
            "SELECT kql_bin(kql_datetime('1970-05-11 13:45:07.456345672'), toIntervalNanosecond(1000000))"
        },
        {
            "print bin(datetime(1970-05-11 13:45:07.456345672), 1microseconds)",
            "SELECT kql_bin(kql_datetime('1970-05-11 13:45:07.456345672'), toIntervalNanosecond(1000))"
        }
})));
