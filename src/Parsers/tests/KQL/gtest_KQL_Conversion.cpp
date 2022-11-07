#include <Parsers/tests/gtest_common.h>

#include <Parsers/Kusto/ParserKQLQuery.h>

INSTANTIATE_TEST_SUITE_P(
    ParserKQLQuery_Conversion,
    ParserTest,
    ::testing::Combine(
        ::testing::Values(std::make_shared<DB::ParserKQLQuery>()),
        ::testing::ValuesIn(std::initializer_list<ParserTestCase>{
        {
            "print tobool(A)",
            "SELECT multiIf(toString(A) = 'true', true, toString(A) = 'false', false, toInt64OrNull(toString(A)) != 0)"
        },
        {
            "print toboolean(A)",
            "SELECT multiIf(toString(A) = 'true', true, toString(A) = 'false', false, toInt64OrNull(toString(A)) != 0)"
        },
        {
            "print todouble(A)",
            "SELECT toFloat64OrNull(toString(A))"
        },
        {
            "print toint(A)",
            "SELECT toInt32OrNull(toString(A))"
        },
        {
            "print tolong(A)",
            "SELECT toInt64OrNull(toString(A))"
        },
        {
            "print toreal(A)",
            "SELECT toFloat64OrNull(toString(A))"
        },
        {
            "print tostring(A)",
            "SELECT ifNull(toString(A), '')"
        },
        {
            "print decimal(123.345)",
            "SELECT toDecimal128(CAST('123.345', 'String'), 32)"
        },
        {
            "print decimal(NULL)",
            "SELECT NULL"
        },
        {
            "print todecimal('123.45')",
            "SELECT if((toTypeName('123.45') = 'String') OR (toTypeName('123.45') = 'FixedString'), toDecimal128OrNull(CAST('123.45', 'String'), abs(34 - CAST(if(position(CAST('123.45', 'String'), 'e') = 0, if(countSubstrings(CAST('123.45', 'String'), '.') = 1, length(substr(CAST('123.45', 'String'), position(CAST('123.45', 'String'), '.') + 1)), 0), toUInt64(multiIf((position(CAST('123.45', 'String'), 'e+') AS x) > 0, substr(CAST('123.45', 'String'), x + 2), (position(CAST('123.45', 'String'), 'e-') AS y) > 0, substr(CAST('123.45', 'String'), y + 2), (position(CAST('123.45', 'String'), 'e-') = 0) AND (position(CAST('123.45', 'String'), 'e+') = 0) AND (position(CAST('123.45', 'String'), 'e') > 0), substr(CAST('123.45', 'String'), position(CAST('123.45', 'String'), 'e') + 1), CAST('0', 'String')))), 'UInt8'))), toDecimal128OrNull(CAST('123.45', 'String'), abs(17 - CAST(if(position(CAST('123.45', 'String'), 'e') = 0, if(countSubstrings(CAST('123.45', 'String'), '.') = 1, length(substr(CAST('123.45', 'String'), position(CAST('123.45', 'String'), '.') + 1)), 0), toUInt64(multiIf(x > 0, substr(CAST('123.45', 'String'), x + 2), y > 0, substr(CAST('123.45', 'String'), y + 2), (position(CAST('123.45', 'String'), 'e-') = 0) AND (position(CAST('123.45', 'String'), 'e+') = 0) AND (position(CAST('123.45', 'String'), 'e') > 0), substr(CAST('123.45', 'String'), position(CAST('123.45', 'String'), 'e') + 1), CAST('0', 'String')))), 'UInt8'))))"
        },
        {
            "print todecimal(NULL)",
            "SELECT NULL"
        },
        {
            "print todecimal(123456.3456)",
            "SELECT if((toTypeName(123456.3456) = 'String') OR (toTypeName(123456.3456) = 'FixedString'), toDecimal128OrNull(CAST('123456.3456', 'String'), abs(34 - CAST(if(position(CAST('123456.3456', 'String'), 'e') = 0, if(countSubstrings(CAST('123456.3456', 'String'), '.') = 1, length(substr(CAST('123456.3456', 'String'), position(CAST('123456.3456', 'String'), '.') + 1)), 0), toUInt64(multiIf((position(CAST('123456.3456', 'String'), 'e+') AS x) > 0, substr(CAST('123456.3456', 'String'), x + 2), (position(CAST('123456.3456', 'String'), 'e-') AS y) > 0, substr(CAST('123456.3456', 'String'), y + 2), (position(CAST('123456.3456', 'String'), 'e-') = 0) AND (position(CAST('123456.3456', 'String'), 'e+') = 0) AND (position(CAST('123456.3456', 'String'), 'e') > 0), substr(CAST('123456.3456', 'String'), position(CAST('123456.3456', 'String'), 'e') + 1), CAST('0', 'String')))), 'UInt8'))), toDecimal128OrNull(CAST('123456.3456', 'String'), abs(17 - CAST(if(position(CAST('123456.3456', 'String'), 'e') = 0, if(countSubstrings(CAST('123456.3456', 'String'), '.') = 1, length(substr(CAST('123456.3456', 'String'), position(CAST('123456.3456', 'String'), '.') + 1)), 0), toUInt64(multiIf(x > 0, substr(CAST('123456.3456', 'String'), x + 2), y > 0, substr(CAST('123456.3456', 'String'), y + 2), (position(CAST('123456.3456', 'String'), 'e-') = 0) AND (position(CAST('123456.3456', 'String'), 'e+') = 0) AND (position(CAST('123456.3456', 'String'), 'e') > 0), substr(CAST('123456.3456', 'String'), position(CAST('123456.3456', 'String'), 'e') + 1), CAST('0', 'String')))), 'UInt8'))))"
        },
        {
            "print todecimal('abc')",
            "SELECT if((toTypeName('abc') = 'String') OR (toTypeName('abc') = 'FixedString'), toDecimal128OrNull(CAST('abc', 'String'), abs(34 - CAST(if(position(CAST('abc', 'String'), 'e') = 0, if(countSubstrings(CAST('abc', 'String'), '.') = 1, length(substr(CAST('abc', 'String'), position(CAST('abc', 'String'), '.') + 1)), 0), toUInt64(multiIf((position(CAST('abc', 'String'), 'e+') AS x) > 0, substr(CAST('abc', 'String'), x + 2), (position(CAST('abc', 'String'), 'e-') AS y) > 0, substr(CAST('abc', 'String'), y + 2), (position(CAST('abc', 'String'), 'e-') = 0) AND (position(CAST('abc', 'String'), 'e+') = 0) AND (position(CAST('abc', 'String'), 'e') > 0), substr(CAST('abc', 'String'), position(CAST('abc', 'String'), 'e') + 1), CAST('0', 'String')))), 'UInt8'))), toDecimal128OrNull(CAST('abc', 'String'), abs(17 - CAST(if(position(CAST('abc', 'String'), 'e') = 0, if(countSubstrings(CAST('abc', 'String'), '.') = 1, length(substr(CAST('abc', 'String'), position(CAST('abc', 'String'), '.') + 1)), 0), toUInt64(multiIf(x > 0, substr(CAST('abc', 'String'), x + 2), y > 0, substr(CAST('abc', 'String'), y + 2), (position(CAST('abc', 'String'), 'e-') = 0) AND (position(CAST('abc', 'String'), 'e+') = 0) AND (position(CAST('abc', 'String'), 'e') > 0), substr(CAST('abc', 'String'), position(CAST('abc', 'String'), 'e') + 1), CAST('0', 'String')))), 'UInt8'))))"
        },
        {
            "print todecimal('1e5')",
            "SELECT if((toTypeName('1e5') = 'String') OR (toTypeName('1e5') = 'FixedString'), toDecimal128OrNull(CAST('1e5', 'String'), abs(34 - CAST(if(position(CAST('1e5', 'String'), 'e') = 0, if(countSubstrings(CAST('1e5', 'String'), '.') = 1, length(substr(CAST('1e5', 'String'), position(CAST('1e5', 'String'), '.') + 1)), 0), toUInt64(multiIf((position(CAST('1e5', 'String'), 'e+') AS x) > 0, substr(CAST('1e5', 'String'), x + 2), (position(CAST('1e5', 'String'), 'e-') AS y) > 0, substr(CAST('1e5', 'String'), y + 2), (position(CAST('1e5', 'String'), 'e-') = 0) AND (position(CAST('1e5', 'String'), 'e+') = 0) AND (position(CAST('1e5', 'String'), 'e') > 0), substr(CAST('1e5', 'String'), position(CAST('1e5', 'String'), 'e') + 1), CAST('0', 'String')))), 'UInt8'))), toDecimal128OrNull(CAST('1e5', 'String'), abs(17 - CAST(if(position(CAST('1e5', 'String'), 'e') = 0, if(countSubstrings(CAST('1e5', 'String'), '.') = 1, length(substr(CAST('1e5', 'String'), position(CAST('1e5', 'String'), '.') + 1)), 0), toUInt64(multiIf(x > 0, substr(CAST('1e5', 'String'), x + 2), y > 0, substr(CAST('1e5', 'String'), y + 2), (position(CAST('1e5', 'String'), 'e-') = 0) AND (position(CAST('1e5', 'String'), 'e+') = 0) AND (position(CAST('1e5', 'String'), 'e') > 0), substr(CAST('1e5', 'String'), position(CAST('1e5', 'String'), 'e') + 1), CAST('0', 'String')))), 'UInt8'))))"
        },
        {
            "print decimal(1e-5)",
            "SELECT toDecimal128(CAST('1e-5', 'String'), 5)"
        },
        {
            "print time(9nanoseconds)",
            "SELECT CAST('9e-09', 'Float64')"
        },
        {
            "print time(1tick)",
            "SELECT CAST('1e-07', 'Float64')"
        }

})));
