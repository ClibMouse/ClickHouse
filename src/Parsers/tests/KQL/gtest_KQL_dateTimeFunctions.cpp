#include <Parsers/tests/gtest_common.h>

#include <Parsers/Kusto/ParserKQLStatement.h>

INSTANTIATE_TEST_SUITE_P(ParserKQLQuery_Datetime, ParserKQLTest,
    ::testing::Combine(
        ::testing::Values(std::make_shared<DB::ParserKQLStatement>()),
        ::testing::ValuesIn(std::initializer_list<ParserTestCase>{
        {
            "print week_of_year(datetime(2020-12-31))",
            "SELECT toWeek(kql_datetime('2020-12-31'), 3, 'UTC') AS print_0"
        },
        {
            "print startofweek(datetime(2017-01-01 10:10:17), -1)",
            "SELECT kql_todatetime(addWeeks(toStartOfWeek(kql_datetime('2017-01-01 10:10:17')), -1)) AS print_0"
        },
        {
            "print startofmonth(datetime(2017-01-01 10:10:17), -1)",
            "SELECT kql_todatetime(addMonths(toStartOfMonth(kql_datetime('2017-01-01 10:10:17')), -1)) AS print_0"
        },
        {
            "print startofday(datetime(2017-01-01 10:10:17), -1)",
            "SELECT kql_todatetime(addDays(toStartOfDay(kql_datetime('2017-01-01 10:10:17')), -1)) AS print_0"
        },
        {
            "print startofyear(datetime(2017-01-01 10:10:17), -1)",
            "SELECT kql_todatetime(addYears(toStartOfYear(kql_datetime('2017-01-01 10:10:17')), -1)) AS print_0"
        },
        {
            "print monthofyear(datetime(2015-12-14))",
            "SELECT toMonth(kql_datetime('2015-12-14')) AS print_0"
        },
        {
            "print hourofday(datetime(2015-12-14 10:54:00))",
            "SELECT toHour(kql_datetime('2015-12-14 10:54:00')) AS print_0"
        },
        {
            "print getyear(datetime(2015-10-12))",
            "SELECT toYear(kql_datetime('2015-10-12')) AS print_0"
        },
        {
            "print getmonth(datetime(2015-10-12))",
            "SELECT toMonth(kql_datetime('2015-10-12')) AS print_0"
        },
        {
            "print dayofyear(datetime(2015-10-12))",
            "SELECT toDayOfYear(kql_datetime('2015-10-12')) AS print_0"
        },
        {
            "print dayofmonth(datetime(2015-10-12))",
            "SELECT toDayOfMonth(kql_datetime('2015-10-12')) AS print_0"
        },
        {
            "print unixtime_seconds_todatetime(1546300899)",
            "SELECT if(toTypeName(assumeNotNull(1546300899)) IN ['Int32', 'Int64', 'Float64', 'UInt32', 'UInt64'], kql_todatetime(1546300899), kql_todatetime(throwIf(true, 'unixtime_seconds_todatetime only accepts int, long and double type of arguments'))) AS print_0"
        },
        {
            "print dayofweek(datetime(2015-12-20))",
            "SELECT (toDayOfWeek(kql_datetime('2015-12-20')) % 7) * toIntervalNanosecond(86400000000000) AS print_0"
        },
        {
            "print now()",
            "SELECT now64(9, 'UTC') AS print_0"
        },
        {
            "print now(1d)",
            "SELECT now64(9, 'UTC') + toIntervalNanosecond(86400000000000) AS print_0"
        },
        {
            "print ago(2d)",
            "SELECT now64(9, 'UTC') + (-1 * toIntervalNanosecond(172800000000000)) AS print_0"
        },  
        {
            "print endofday(datetime(2017-01-01 10:10:17), -1)",
            "SELECT kql_todatetime(addDays(toStartOfDay(kql_datetime('2017-01-01 10:10:17')), -1 + 1)) - toIntervalNanosecond(100) AS print_0"
        },
        {
            "print endofday(datetime(2017-01-01 10:10:17), 1)",
            "SELECT kql_todatetime(addDays(toStartOfDay(kql_datetime('2017-01-01 10:10:17')), 1 + 1)) - toIntervalNanosecond(100) AS print_0"
        },
        {
            "print endofmonth(datetime(2017-01-01 10:10:17), -1)",
            "SELECT kql_todatetime(addMonths(toStartOfMonth(kql_datetime('2017-01-01 10:10:17')), -1 + 1)) - toIntervalNanosecond(100) AS print_0"
        },
        {
            "print endofmonth(datetime(2017-01-01 10:10:17), 1)",
            "SELECT kql_todatetime(addMonths(toStartOfMonth(kql_datetime('2017-01-01 10:10:17')), 1 + 1)) - toIntervalNanosecond(100) AS print_0"
        },
        {
            "print endofweek(datetime(2017-01-01 10:10:17), -1)",
            "SELECT kql_todatetime(addWeeks(toStartOfWeek(kql_datetime('2017-01-01 10:10:17')), -1 + 1)) - toIntervalNanosecond(100) AS print_0"
        },
        {
            "print endofweek(datetime(2017-01-01 10:10:17), 1)",
            "SELECT kql_todatetime(addWeeks(toStartOfWeek(kql_datetime('2017-01-01 10:10:17')), 1 + 1)) - toIntervalNanosecond(100) AS print_0"
        },
        {
            "print endofyear(datetime(2017-01-01 10:10:17), -1) ",
            "SELECT kql_todatetime(addYears(toStartOfYear(kql_datetime('2017-01-01 10:10:17')), -1 + 1)) - toIntervalNanosecond(100) AS print_0"
        },
        {
            "print endofyear(datetime(2017-01-01 10:10:17), 1)" ,
            "SELECT kql_todatetime(addYears(toStartOfYear(kql_datetime('2017-01-01 10:10:17')), 1 + 1)) - toIntervalNanosecond(100) AS print_0"
        },
        {
            "print make_datetime(2017,10,01)",
            "SELECT if(((2017 >= 1900) AND (2017 <= 2261)) AND ((10 >= 1) AND (10 <= 12)) AND ((0 >= 0) AND (0 <= 59)) AND ((0 >= 0) AND (0 <= 59)) AND (0 >= 0) AND (0 < 60) AND (toModifiedJulianDayOrNull(concat(leftPad(toString(2017), 4, '0'), '-', leftPad(toString(10), 2, '0'), '-', leftPad(toString(1), 2, '0'))) IS NOT NULL), toDateTime64OrNull(toString(makeDateTime64(2017, 10, 1, 0, 0, truncate(0), (0 - truncate(0)) * 10000000., 7, 'UTC')), 9, 'UTC'), NULL) AS print_0"
        },
        {
            "print make_datetime(2017,10,01,12,10)",
            "SELECT if(((2017 >= 1900) AND (2017 <= 2261)) AND ((10 >= 1) AND (10 <= 12)) AND ((12 >= 0) AND (12 <= 59)) AND ((10 >= 0) AND (10 <= 59)) AND (0 >= 0) AND (0 < 60) AND (toModifiedJulianDayOrNull(concat(leftPad(toString(2017), 4, '0'), '-', leftPad(toString(10), 2, '0'), '-', leftPad(toString(1), 2, '0'))) IS NOT NULL), toDateTime64OrNull(toString(makeDateTime64(2017, 10, 1, 12, 10, truncate(0), (0 - truncate(0)) * 10000000., 7, 'UTC')), 9, 'UTC'), NULL) AS print_0"
        },
        {
            "print make_datetime(2017,10,01,12,11,0.1234567)",
            "SELECT if(((2017 >= 1900) AND (2017 <= 2261)) AND ((10 >= 1) AND (10 <= 12)) AND ((12 >= 0) AND (12 <= 59)) AND ((11 >= 0) AND (11 <= 59)) AND (0.1234567 >= 0) AND (0.1234567 < 60) AND (toModifiedJulianDayOrNull(concat(leftPad(toString(2017), 4, '0'), '-', leftPad(toString(10), 2, '0'), '-', leftPad(toString(1), 2, '0'))) IS NOT NULL), toDateTime64OrNull(toString(makeDateTime64(2017, 10, 1, 12, 11, truncate(0.1234567), (0.1234567 - truncate(0.1234567)) * 10000000., 7, 'UTC')), 9, 'UTC'), NULL) AS print_0"
        },
        {
            "print unixtime_microseconds_todatetime(1546300800000000)",
            "SELECT kql_todatetime(fromUnixTimestamp64Micro(1546300800000000, 'UTC')) AS print_0"
        },
        {
            "print unixtime_milliseconds_todatetime(1546300800000)",
            "SELECT kql_todatetime(fromUnixTimestamp64Milli(1546300800000, 'UTC')) AS print_0"
        },
        {
            "print unixtime_nanoseconds_todatetime(1546300800000000000)",
            "SELECT kql_todatetime(fromUnixTimestamp64Nano(1546300800000000000, 'UTC')) AS print_0"
        },
        {
            "print datetime_diff('year',datetime(2017-01-01),datetime(2000-12-31))",
            "SELECT dateDiff('year', kql_datetime('2000-12-31'), kql_datetime('2017-01-01')) AS print_0"
        },
        {
            "print datetime_diff('minute',datetime(2017-10-30 23:05:01),datetime(2017-10-30 23:00:59))",
            "SELECT dateDiff('minute', kql_datetime('2017-10-30 23:00:59'), kql_datetime('2017-10-30 23:05:01')) AS print_0"
        },
        {
            "print datetime(null)",
            "SELECT kql_datetime(NULL) AS print_0"
        },
        {
            "print datetime('2014-05-25T08:20:03.123456Z')",
            "SELECT kql_datetime('2014-05-25T08:20:03.123456Z') AS print_0"
        },
        {
            "print datetime(2015-12-14 18:54)",
            "SELECT kql_datetime('2015-12-14 18:54') AS print_0"
        },
        {
            "print datetime(2015-12-31 23:59:59.9)",
            "SELECT kql_datetime('2015-12-31 23:59:59.9') AS print_0"
        },
        {
            "print datetime(\"2015-12-31 23:59:59.9\")",
            "SELECT kql_datetime('2015-12-31 23:59:59.9') AS print_0"
        },
        {
            "print datetime('2015-12-31 23:59:59.9')",
            "SELECT kql_datetime('2015-12-31 23:59:59.9') AS print_0"
        },
        {
            "print make_timespan(67,12,30,59.9799)",
            "SELECT (((67 * toIntervalNanosecond(86400000000000)) + (12 * toIntervalNanosecond(3600000000000))) + (30 * toIntervalNanosecond(60000000000))) + (59.9799 * toIntervalNanosecond(1000000000)) AS print_0"
        },
        {
            "print  todatetime('2014-05-25T08:20:03.123456Z')",
            "SELECT kql_todatetime('2014-05-25T08:20:03.123456Z') AS print_0"
        },
        {
            "print format_datetime(todatetime('2009-06-15T13:45:30.6175425'), 'yy-M-dd [H:mm:ss.fff]')",
            "SELECT concat(substring(toString(formatDateTime(kql_todatetime('2009-06-15T13:45:30.6175425'), '%y-%m-%d [%H:%i:%S.]')), 1, position(toString(formatDateTime(kql_todatetime('2009-06-15T13:45:30.6175425'), '%y-%m-%d [%H:%i:%S.]')), '.')), substring(substring(toString(kql_todatetime('2009-06-15T13:45:30.6175425')), position(toString(kql_todatetime('2009-06-15T13:45:30.6175425')), '.') + 1), 1, 3), substring(toString(formatDateTime(kql_todatetime('2009-06-15T13:45:30.6175425'), '%y-%m-%d [%H:%i:%S.]')), position(toString(formatDateTime(kql_todatetime('2009-06-15T13:45:30.6175425'), '%y-%m-%d [%H:%i:%S.]')), '.') + 1, length(toString(formatDateTime(kql_todatetime('2009-06-15T13:45:30.6175425'), '%y-%m-%d [%H:%i:%S.]'))))) AS print_0"
        },
        {
            "print format_datetime(datetime(2015-12-14 02:03:04.12345), 'y-M-d h:m:s tt')",
            "SELECT formatDateTime(kql_datetime('2015-12-14 02:03:04.12345'), '%y-%m-%e %I:%i:%S %p') AS print_0"
        },
        {
            "print format_timespan(time(1d), 'd-[hh:mm:ss]')",
            "SELECT concat(if(length(toString(intDiv(toIntervalNanosecond(86400000000000), toIntervalNanosecond(86400000000000)))) < 1, leftPad(toString(intDiv(toIntervalNanosecond(86400000000000), toIntervalNanosecond(86400000000000))), 1, '0'), toString(intDiv(toIntervalNanosecond(86400000000000), toIntervalNanosecond(86400000000000)))), '-', '[', if(length(toString(intDiv(toIntervalNanosecond(86400000000000), toIntervalNanosecond(3600000000000)) % 24)) < 2, leftPad(toString(intDiv(toIntervalNanosecond(86400000000000), toIntervalNanosecond(3600000000000)) % 24), 2, '0'), toString(intDiv(toIntervalNanosecond(86400000000000), toIntervalNanosecond(3600000000000)) % 24)), ':', if(length(toString(intDiv(toIntervalNanosecond(86400000000000), toIntervalNanosecond(60000000000)) % 60)) < 2, leftPad(toString(intDiv(toIntervalNanosecond(86400000000000), toIntervalNanosecond(60000000000)) % 60), 2, '0'), toString(intDiv(toIntervalNanosecond(86400000000000), toIntervalNanosecond(60000000000)) % 60)), ':', if(length(toString(intDiv(toIntervalNanosecond(86400000000000), toIntervalNanosecond(1000000000)) % 60)) < 2, leftPad(toString(intDiv(toIntervalNanosecond(86400000000000), toIntervalNanosecond(1000000000)) % 60), 2, '0'), toString(intDiv(toIntervalNanosecond(86400000000000), toIntervalNanosecond(1000000000)) % 60)), ']', '') AS print_0"
        },
        {
            "print format_timespan(time('12:30:55.123'), 'ddddd-[hh:mm:ss.ffff]')",
            "SELECT concat(if(length(toString(intDiv(toIntervalNanosecond(45055123000000), toIntervalNanosecond(86400000000000)))) < 5, leftPad(toString(intDiv(toIntervalNanosecond(45055123000000), toIntervalNanosecond(86400000000000))), 5, '0'), toString(intDiv(toIntervalNanosecond(45055123000000), toIntervalNanosecond(86400000000000)))), '-', '[', if(length(toString(intDiv(toIntervalNanosecond(45055123000000), toIntervalNanosecond(3600000000000)) % 24)) < 2, leftPad(toString(intDiv(toIntervalNanosecond(45055123000000), toIntervalNanosecond(3600000000000)) % 24), 2, '0'), toString(intDiv(toIntervalNanosecond(45055123000000), toIntervalNanosecond(3600000000000)) % 24)), ':', if(length(toString(intDiv(toIntervalNanosecond(45055123000000), toIntervalNanosecond(60000000000)) % 60)) < 2, leftPad(toString(intDiv(toIntervalNanosecond(45055123000000), toIntervalNanosecond(60000000000)) % 60), 2, '0'), toString(intDiv(toIntervalNanosecond(45055123000000), toIntervalNanosecond(60000000000)) % 60)), ':', if(length(toString(intDiv(toIntervalNanosecond(45055123000000), toIntervalNanosecond(1000000000)) % 60)) < 2, leftPad(toString(intDiv(toIntervalNanosecond(45055123000000), toIntervalNanosecond(1000000000)) % 60), 2, '0'), toString(intDiv(toIntervalNanosecond(45055123000000), toIntervalNanosecond(1000000000)) % 60)), '.', if(length(substring(toString(intDiv(toIntervalNanosecond(45055123000000), toIntervalNanosecond(100)) % 10000000), 1, 4)) < 4, rightPad(substring(toString(intDiv(toIntervalNanosecond(45055123000000), toIntervalNanosecond(100)) % 10000000), 1, 4), 4, '0'), substring(toString(intDiv(toIntervalNanosecond(45055123000000), toIntervalNanosecond(100)) % 10000000), 1, 4)), ']', '') AS print_0"
        },
        {
            "print format_timespan(time('29.09:00:05.12345'), 'dd.hh:mm:ss:FF')",
            "SELECT concat(if(length(toString(intDiv(toIntervalNanosecond(2538005123450000), toIntervalNanosecond(86400000000000)))) < 2, leftPad(toString(intDiv(toIntervalNanosecond(2538005123450000), toIntervalNanosecond(86400000000000))), 2, '0'), toString(intDiv(toIntervalNanosecond(2538005123450000), toIntervalNanosecond(86400000000000)))), '.', if(length(toString(intDiv(toIntervalNanosecond(2538005123450000), toIntervalNanosecond(3600000000000)) % 24)) < 2, leftPad(toString(intDiv(toIntervalNanosecond(2538005123450000), toIntervalNanosecond(3600000000000)) % 24), 2, '0'), toString(intDiv(toIntervalNanosecond(2538005123450000), toIntervalNanosecond(3600000000000)) % 24)), ':', if(length(toString(intDiv(toIntervalNanosecond(2538005123450000), toIntervalNanosecond(60000000000)) % 60)) < 2, leftPad(toString(intDiv(toIntervalNanosecond(2538005123450000), toIntervalNanosecond(60000000000)) % 60), 2, '0'), toString(intDiv(toIntervalNanosecond(2538005123450000), toIntervalNanosecond(60000000000)) % 60)), ':', if(length(toString(intDiv(toIntervalNanosecond(2538005123450000), toIntervalNanosecond(1000000000)) % 60)) < 2, leftPad(toString(intDiv(toIntervalNanosecond(2538005123450000), toIntervalNanosecond(1000000000)) % 60), 2, '0'), toString(intDiv(toIntervalNanosecond(2538005123450000), toIntervalNanosecond(1000000000)) % 60)), ':', substring(toString(intDiv(toIntervalNanosecond(2538005123450000), toIntervalNanosecond(100)) % 10000000), 1, 2), '') AS print_0"
        },
        {
            "print format_timespan(time('29.09:00:05.12345'), 'ddd.h:mm:ss [fffffff]');",
            "SELECT concat(if(length(toString(intDiv(toIntervalNanosecond(2538005123450000), toIntervalNanosecond(86400000000000)))) < 3, leftPad(toString(intDiv(toIntervalNanosecond(2538005123450000), toIntervalNanosecond(86400000000000))), 3, '0'), toString(intDiv(toIntervalNanosecond(2538005123450000), toIntervalNanosecond(86400000000000)))), '.', if(length(toString(intDiv(toIntervalNanosecond(2538005123450000), toIntervalNanosecond(3600000000000)) % 24)) < 1, leftPad(toString(intDiv(toIntervalNanosecond(2538005123450000), toIntervalNanosecond(3600000000000)) % 24), 1, '0'), toString(intDiv(toIntervalNanosecond(2538005123450000), toIntervalNanosecond(3600000000000)) % 24)), ':', if(length(toString(intDiv(toIntervalNanosecond(2538005123450000), toIntervalNanosecond(60000000000)) % 60)) < 2, leftPad(toString(intDiv(toIntervalNanosecond(2538005123450000), toIntervalNanosecond(60000000000)) % 60), 2, '0'), toString(intDiv(toIntervalNanosecond(2538005123450000), toIntervalNanosecond(60000000000)) % 60)), ':', if(length(toString(intDiv(toIntervalNanosecond(2538005123450000), toIntervalNanosecond(1000000000)) % 60)) < 2, leftPad(toString(intDiv(toIntervalNanosecond(2538005123450000), toIntervalNanosecond(1000000000)) % 60), 2, '0'), toString(intDiv(toIntervalNanosecond(2538005123450000), toIntervalNanosecond(1000000000)) % 60)), ' ', '[', if(length(substring(toString(intDiv(toIntervalNanosecond(2538005123450000), toIntervalNanosecond(100)) % 10000000), 1, 7)) < 7, rightPad(substring(toString(intDiv(toIntervalNanosecond(2538005123450000), toIntervalNanosecond(100)) % 10000000), 1, 7), 7, '0'), substring(toString(intDiv(toIntervalNanosecond(2538005123450000), toIntervalNanosecond(100)) % 10000000), 1, 7)), ']', '') AS print_0"
        },
        {
            "print datetime_part('day', datetime(2017-10-30 01:02:03.7654321))",
            "SELECT formatDateTime(kql_datetime('2017-10-30 01:02:03.7654321'), '%e') AS print_0"
        },
        {
            "print datetime_add('day',1,datetime(2017-10-30 01:02:03.7654321))",
            "SELECT kql_datetime(kql_datetime('2017-10-30 01:02:03.7654321') + toIntervalDay(1)) AS print_0"
        },
        {
            "print totimespan(time(1d))",
            "SELECT kql_totimespan(toIntervalNanosecond(86400000000000)) AS print_0"
        },
        {
            "print totimespan('0.01:34:23')",
            "SELECT kql_totimespan('0.01:34:23') AS print_0"
        },
        {
            "print totimespan(time('-1:12:34'))",
            "SELECT kql_totimespan(toIntervalNanosecond(-4354000000000)) AS print_0"
        },
        {
            "print totimespan(-1d)",
            "SELECT kql_totimespan(-toIntervalNanosecond(86400000000000)) AS print_0"
        },
        {
            "print totimespan('abc')",
            "SELECT kql_totimespan('abc') AS print_0"
        },
        {
            "print time(2)",
            "SELECT toIntervalNanosecond(172800000000000) AS print_0"
        },
        {
            "hits | project bin(todatetime(EventTime), 1m)",
            "SELECT kql_bin(kql_todatetime(EventTime), toIntervalNanosecond(60000000000)) AS Column1\nFROM hits"
        }

})));
