#include <Parsers/tests/gtest_common.h>

#include <Parsers/Kusto/ParserKQLQuery.h>

INSTANTIATE_TEST_SUITE_P(ParserKQLQuery_DataType, ParserKQLTest,
    ::testing::Combine(
        ::testing::Values(std::make_shared<DB::ParserKQLQuery>()),
        ::testing::ValuesIn(std::initializer_list<ParserTestCase>{
        {
            "print dynamic(null)",
            "SELECT NULL AS print_0"
        },
        {
            "print dynamic(1)",
            "SELECT 1 AS print_0"
        },
        {
            "print dynamic(datetime(1))",
            "SELECT kql_datetime(1) AS print_0"
        },
        {
            "print dynamic(timespan(1d))",
            "SELECT toIntervalNanosecond(86400000000000) AS print_0"
        },
        {
            "print dynamic(parse_ipv4('127.0.0.1'))",
            "throws AS print_0"
        },
        {
            "print dynamic({ \"a\": 9 })",
            "throws AS print_0"
        },
        {
            "print dynamic([1, 2, 3])",
            "SELECT [1, 2, 3] AS print_0"
        },
        {
            "print dynamic([1, dynamic([2]), 3])",
            "SELECT [1, [2], 3] AS print_0"
        },
        {
            "print dynamic([[1], [2], [3]])",
            "SELECT [[1], [2], [3]] AS print_0"
        },
        {
            "print dynamic(['a', \"b\", 'c'])",
            "SELECT ['a', 'b', 'c'] AS print_0"
        },
        {
            "print dynamic([1, 'a', true, false])",
            "SELECT [1, 'a', true, false] AS print_0"
        },
        {
            "print dynamic([date(1), time(1d), 1, 2])",
            "SELECT [kql_datetime(1), toIntervalNanosecond(86400000000000), 1, 2] AS print_0"
        },
        {
            "print time('13:00:40.00000')",
            "SELECT toIntervalNanosecond(46840000000000) AS print_0"
        },
        {
            "print timespan('12.23:12:23');",
            "SELECT toIntervalNanosecond(1120343000000000) AS print_0"
        }
})));
