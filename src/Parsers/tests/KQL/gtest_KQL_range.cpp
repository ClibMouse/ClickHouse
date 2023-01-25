#include <Parsers/tests/gtest_common.h>

#include <Parsers/Kusto/ParserKQLQuery.h>

INSTANTIATE_TEST_SUITE_P(ParserKQLQuery_Range, ParserTest,
    ::testing::Combine(
        ::testing::Values(std::make_shared<DB::ParserKQLQuery>()),
        ::testing::ValuesIn(std::initializer_list<ParserTestCase>{
        {
            "print range(1, 10, 2)",
            "SELECT kql_range(1, 10, 2)"
        },
        {
            "print range(1, 10)",
            "SELECT kql_range(1, 10)"
        },
        {
            "print range(1.2, 10.3, 2.2)",
            "SELECT kql_range(1.2, 10.3, 2.2)"
        },
        {
            "print range(1.2, 10.3, 2)",
            "SELECT kql_range(1.2, 10.3, 2)"
        },
        {
            "print range(1.2, 10,2.2)",
            "SELECT kql_range(1.2, 10, 2.2)"
        },
        {
            "print range(1, 10, 2.2)",
            "SELECT kql_range(1, 10, 2.2)"
        },
        {
            "print range(1, 10.5, 2.2)",
            "SELECT kql_range(1, 10.5, 2.2)"
        },
        {
            "print range(1.1, 10 ,2.2)",
            "SELECT kql_range(1.1, 10, 2.2)"
        },
        {
            "print range(1.2, 10, 2)",
            "SELECT kql_range(1.2, 10, 2)"
        },
        {
            "print range(datetime('2001-01-01'), datetime('2001-01-02'), 5h)",
            "SELECT kql_range(kql_datetime('2001-01-01'), kql_datetime('2001-01-02'), toIntervalNanosecond(18000000000000))"
        },
        {
            "print range(datetime('2001-01-01'), datetime('2001-01-02'))",
            "SELECT kql_range(kql_datetime('2001-01-01'), kql_datetime('2001-01-02'))"
        },
        {
            "print range(1h, 5h, 2h)",
            "SELECT kql_range(toIntervalNanosecond(3600000000000), toIntervalNanosecond(18000000000000), toIntervalNanosecond(7200000000000))"
        },
        {
            "print range(1.5h, 5h, 2h)",
            "SELECT kql_range(toIntervalNanosecond(5400000000000), toIntervalNanosecond(18000000000000), toIntervalNanosecond(7200000000000))"
        },
        {
            "print range(ago(1d),now(),1d)",
            "SELECT kql_range(now64(9, 'UTC') + (-1 * toIntervalNanosecond(86400000000000)), now64(9, 'UTC'), toIntervalNanosecond(86400000000000))"
        },
        {
            "print range(endofday(datetime(2017-01-01 10:10:17)), endofday(datetime(2017-01-03 10:10:17)), 1d)",
            "SELECT kql_range(kql_todatetime(addDays(toStartOfDay(kql_datetime('2017-01-01 10:10:17')), 0 + 1)) - toIntervalNanosecond(100), kql_todatetime(addDays(toStartOfDay(kql_datetime('2017-01-03 10:10:17')), 0 + 1)) - toIntervalNanosecond(100), toIntervalNanosecond(86400000000000))"
        }
})));
