#include <Parsers/tests/gtest_common.h>

#include <Parsers/Kusto/ParserKQLStatement.h>

INSTANTIATE_TEST_SUITE_P(ParserKQLQuery_General, ParserTest,
    ::testing::Combine(
        ::testing::Values(std::make_shared<DB::ParserKQLStatement>()),
        ::testing::ValuesIn(std::initializer_list<ParserTestCase>{
        {
            "print case(5 <= 10, 'A', 12 <= 20, 'B', 22 <= 30, 'C', 'D')",
            "SELECT multiIf(5 <= 10, 'A', 12 <= 20, 'B', 22 <= 30, 'C', 'D') AS print_0"
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
            "print bin_at(6.5, 2.5, 7)",
            "SELECT kql_bin_at(6.5, 2.5, 7) AS print_0"
        },
        {
            "print bin_at(1h, 1d, 12h)",
            "SELECT kql_bin_at(toIntervalNanosecond(3600000000000), toIntervalNanosecond(86400000000000), toIntervalNanosecond(43200000000000)) AS print_0"
        },
        {
            "print bin_at(datetime(2017-05-15 10:20:00.0), 1d, datetime(1970-01-01 12:00:00.0))",
            "SELECT kql_bin_at(kql_datetime('2017-05-15 10:20:00.0'), toIntervalNanosecond(86400000000000), kql_datetime('1970-01-01 12:00:00.0')) AS print_0"
        },
        {
            "print bin(4.5, 1)",
            "SELECT kql_bin(4.5, 1) AS print_0"
        },
        {
            "print bin(4.5, -1)",
            "SELECT kql_bin(4.5, -1) AS print_0"
        },
        {
            "print bin(time(16d), 7d)",
            "SELECT kql_bin(toIntervalNanosecond(1382400000000000), toIntervalNanosecond(604800000000000)) AS print_0"
        },
        {
            "print bin(datetime(1970-05-11 13:45:07), 1d)",
            "SELECT kql_bin(kql_datetime('1970-05-11 13:45:07'), toIntervalNanosecond(86400000000000)) AS print_0"
        },
        {
            "print bin(datetime(1970-05-11 13:45:07.456345672), 1ms)",
            "SELECT kql_bin(kql_datetime('1970-05-11 13:45:07.456345672'), toIntervalNanosecond(1000000)) AS print_0"
        },
        {
            "print bin(datetime(1970-05-11 13:45:07.456345672), 1microseconds)",
            "SELECT kql_bin(kql_datetime('1970-05-11 13:45:07.456345672'), toIntervalNanosecond(1000)) AS print_0"
        },
        {
            "print lookup('dictionary_table', 'value', '1')",
            "SELECT dictGet('dictionary_table', 'value', '1') AS print_0"
        },
        {
            "print lookup('dictionary_table', 'value', '100', 'default')",
            "SELECT dictGetOrDefault('dictionary_table', 'value', '100', 'default') AS print_0"
        },
        {
            "T | print 1",
            "throws AS print_0"
        },
        {
            "T | range from 1 to 5 step 1",
            "throws"
        },
        {
            "T |",
            "throws"
        },
        {
            "print t = gettype(1)",
            "SELECT kql_gettype(1) AS t"
        },
        {
            "Customers | project t = gettype(FirstName)",
            "SELECT kql_gettype(FirstName) AS t\nFROM Customers"
        },
        {
            "print x = 5 | extend a = toscalar(print 5, 'asd' | project y = strcat(print_0, print_1));",
            "SELECT\n    * EXCEPT a,\n    (\n        SELECT tuple(*)\n        FROM\n        (\n            SELECT concat(ifNull(kql_tostring(print_0), ''), ifNull(kql_tostring(print_1), ''), '') AS y\n            FROM\n            (\n                SELECT\n                    5 AS print_0,\n                    'asd' AS print_1\n            )\n        )\n        LIMIT 1\n    ).1 AS a\nFROM\n(\n    SELECT 5 AS x\n)"
        }
})));
