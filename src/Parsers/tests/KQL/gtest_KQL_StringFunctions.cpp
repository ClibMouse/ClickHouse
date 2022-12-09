#include <Parsers/tests/gtest_common.h>

#include <Parsers/Kusto/ParserKQLQuery.h>

INSTANTIATE_TEST_SUITE_P(ParserKQLQuery_String, ParserKQLTest,
    ::testing::Combine(
        ::testing::Values(std::make_shared<DB::ParserKQLQuery>()),
        ::testing::ValuesIn(std::initializer_list<ParserTestCase>{
        {
            "print base64_encode_fromguid(A)",
            "SELECT if(toTypeName(A) NOT IN ['UUID', 'Nullable(UUID)'], toString(throwIf(true, 'Expected guid as argument')), base64Encode(UUIDStringToNum(toString(A), 2)))"
        },
        {
            "print base64_decode_toguid(A)",
            "SELECT toUUIDOrNull(UUIDNumToString(toFixedString(base64Decode(A), 16), 2))"
        },
        {
            "print base64_decode_toarray('S3VzdG8=')",
            "SELECT arrayMap(x -> reinterpretAsUInt8(x), splitByRegexp('', base64Decode('S3VzdG8=')))"
        },
        {
            "print replace_regex('Hello, World!', '.', '\\0\\0')",
            "SELECT replaceRegexpAll('Hello, World!', '.', '\\0\\0')"
        },
        {
            "print idx = has_any_index('this is an example', dynamic(['this', 'example'])) ",
            "SELECT if(empty(['this', 'example']), -1, indexOf(arrayMap(x -> (x IN splitByChar(' ', 'this is an example')), if(empty(['this', 'example']), [''], arrayMap(x -> toString(x), ['this', 'example']))), 1) - 1) AS idx"
        },
        {
            "print idx = has_any_index('this is an example', dynamic([]))",
            "SELECT if(empty([]), -1, indexOf(arrayMap(x -> (x IN splitByChar(' ', 'this is an example')), if(empty([]), [''], arrayMap(x -> toString(x), []))), 1) - 1) AS idx"
        },
        {
            "print translate('krasp', 'otsku', 'spark')",
            "SELECT if(length('otsku') = 0, '', translate('spark', 'krasp', multiIf(length('otsku') = 0, 'krasp', (length('krasp') - length('otsku')) > 0, concat('otsku', repeat(substr('otsku', length('otsku'), 1), toUInt16(length('krasp') - length('otsku')))), (length('krasp') - length('otsku')) < 0, substr('otsku', 1, length('krasp')), 'otsku')))"
        },
        {
            "print trim_start('[^\\w]+', strcat('-  ','Te st1','// $'))",
            "SELECT replaceRegexpOne(concat(ifNull(kql_tostring('-  '), ''), ifNull(kql_tostring('Te st1'), ''), ifNull(kql_tostring('// $'), ''), ''), concat('^', '[^\\\\w]+'), '')"
        },
        {
            "print trim_end('.com', 'bing.com')",
            "SELECT replaceRegexpOne('bing.com', concat('.com', '$'), '')"
        },
        {
            "print trim('--', '--https://bing.com--')",
            "SELECT replaceRegexpOne(replaceRegexpOne('--https://bing.com--', concat('--', '$'), ''), concat('^', '--'), '')"
        },
        {
            "print bool(1)",
            "SELECT if((toTypeName(1) = 'IntervalNanosecond') OR ((accurateCastOrNull(1, 'Bool') IS NULL) != (1 IS NULL)), accurateCastOrNull(throwIf(true, 'Failed to parse Bool literal'), 'Bool'), accurateCastOrNull(1, 'Bool'))"
        },
        {
            "print datetime(2015-12-31 23:59:59.9)",
            "SELECT parseDateTime64BestEffortOrNull('2015-12-31 23:59:59.9', 9, 'UTC')"
        },
        {
            "print datetime(\"2015-12-31 23:59:59.9\")",
            "SELECT parseDateTime64BestEffortOrNull('2015-12-31 23:59:59.9', 9, 'UTC')"
        },
        {
            "print datetime('2015-12-31 23:59:59.9')",
            "SELECT parseDateTime64BestEffortOrNull('2015-12-31 23:59:59.9', 9, 'UTC')"
        },
        {
            "print guid(74be27de-1e4e-49d9-b579-fe0b331d3642)",
            "SELECT toUUIDOrNull('74be27de-1e4e-49d9-b579-fe0b331d3642')"
        },
        {
            "print guid('74be27de-1e4e-49d9-b579-fe0b331d3642')",
            "SELECT toUUIDOrNull('74be27de-1e4e-49d9-b579-fe0b331d3642')"
        },
        {
            "print guid('74be27de1e4e49d9b579fe0b331d3642')",
            "SELECT toUUIDOrNull('74be27de1e4e49d9b579fe0b331d3642')"
        },
        {
            "print int(32.5)",
            "SELECT if((toTypeName(32.5) = 'IntervalNanosecond') OR ((accurateCastOrNull(32.5, 'Int32') IS NULL) != (32.5 IS NULL)), accurateCastOrNull(throwIf(true, 'Failed to parse Int32 literal'), 'Int32'), accurateCastOrNull(32.5, 'Int32'))"
        },
        {
            "print long(32.5)",
            "SELECT if((toTypeName(32.5) = 'IntervalNanosecond') OR ((accurateCastOrNull(32.5, 'Int64') IS NULL) != (32.5 IS NULL)), accurateCastOrNull(throwIf(true, 'Failed to parse Int64 literal'), 'Int64'), accurateCastOrNull(32.5, 'Int64'))"
        },
        {
            "print real(32.5)",
            "SELECT if((toTypeName(32.5) = 'IntervalNanosecond') OR ((accurateCastOrNull(32.5, 'Float64') IS NULL) != (32.5 IS NULL)), accurateCastOrNull(throwIf(true, 'Failed to parse Float64 literal'), 'Float64'), accurateCastOrNull(32.5, 'Float64'))"
        },
        {
            "print time('1.22:34:8.128')",
            "SELECT toIntervalNanosecond(167648128000000)"
        },
        {
            "print time('1d')",
            "SELECT toIntervalNanosecond(86400000000000)"
        },
        {
            "print time('1.5d')",
            "SELECT toIntervalNanosecond(129600000000000)"
        },
        {
            "print timespan('1.5d')",
            "SELECT toIntervalNanosecond(129600000000000)"
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
            "SELECT kql_bin_at(parseDateTime64BestEffortOrNull('2017-05-15 10:20:00.0', 9, 'UTC'), toIntervalNanosecond(86400000000000), parseDateTime64BestEffortOrNull('1970-01-01 12:00:00.0', 9, 'UTC')) AS res"
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
            "SELECT kql_bin(parseDateTime64BestEffortOrNull('1970-05-11 13:45:07', 9, 'UTC'), toIntervalNanosecond(86400000000000))"
        },
        {
            "print extract('x=([0-9.]+)', 1, 'hello x=456|wo' , typeof(bool));",
            "SELECT accurateCastOrNull(toInt64OrNull(kql_extract('hello x=456|wo', 'x=([0-9.]+)', 1)), 'Boolean')"
        },
        {
            "print extract('x=([0-9.]+)', 1, 'hello x=456|wo' , typeof(date));",
            "SELECT accurateCastOrNull(kql_extract('hello x=456|wo', 'x=([0-9.]+)', 1), 'DateTime')"
        },
        {
            "print extract('x=([0-9.]+)', 1, 'hello x=456|wo' , typeof(guid));",
            "SELECT accurateCastOrNull(kql_extract('hello x=456|wo', 'x=([0-9.]+)', 1), 'UUID')"
        },
        {
            "print extract('x=([0-9.]+)', 1, 'hello x=456|wo' , typeof(int));",
            "SELECT accurateCastOrNull(kql_extract('hello x=456|wo', 'x=([0-9.]+)', 1), 'Int32')"
        },
        {
            "print extract('x=([0-9.]+)', 1, 'hello x=456|wo' , typeof(long));",
            "SELECT accurateCastOrNull(kql_extract('hello x=456|wo', 'x=([0-9.]+)', 1), 'Int64')"
        },
        {
            "print extract('x=([0-9.]+)', 1, 'hello x=456|wo' , typeof(real));",
            "SELECT accurateCastOrNull(kql_extract('hello x=456|wo', 'x=([0-9.]+)', 1), 'Float64')"
        },
        {
            "print extract('x=([0-9.]+)', 1, 'hello x=456|wo' , typeof(decimal));",
            "SELECT toDecimal128OrNull(if(countSubstrings(kql_extract('hello x=456|wo', 'x=([0-9.]+)', 1), '.') > 1, NULL, kql_extract('hello x=456|wo', 'x=([0-9.]+)', 1)), length(substr(kql_extract('hello x=456|wo', 'x=([0-9.]+)', 1), position(kql_extract('hello x=456|wo', 'x=([0-9.]+)', 1), '.') + 1)))"
        },
        {
            "print parse_version('1.2.3.40')",
            "SELECT if((length(splitByChar('.', '1.2.3.40')) > 4) OR (length(splitByChar('.', '1.2.3.40')) < 1) OR (match('1.2.3.40', '.*[a-zA-Z]+.*') = 1) OR empty('1.2.3.40') OR hasAll(splitByChar('.', '1.2.3.40'), ['']), toDecimal128OrNull('NULL', 0), toDecimal128OrNull(substring(arrayStringConcat(arrayMap(x -> leftPad(x, 8, '0'), arrayMap(x -> if(empty(x), '0', x), arrayResize(splitByChar('.', '1.2.3.40'), 4)))), 8), 0))"
        },
        {
            "print parse_version('1')",
            "SELECT if((length(splitByChar('.', '1')) > 4) OR (length(splitByChar('.', '1')) < 1) OR (match('1', '.*[a-zA-Z]+.*') = 1) OR empty('1') OR hasAll(splitByChar('.', '1'), ['']), toDecimal128OrNull('NULL', 0), toDecimal128OrNull(substring(arrayStringConcat(arrayMap(x -> leftPad(x, 8, '0'), arrayMap(x -> if(empty(x), '0', x), arrayResize(splitByChar('.', '1'), 4)))), 8), 0))"
        },
        {
            "print parse_version('')",
            "SELECT if((length(splitByChar('.', '')) > 4) OR (length(splitByChar('.', '')) < 1) OR (match('', '.*[a-zA-Z]+.*') = 1) OR empty('') OR hasAll(splitByChar('.', ''), ['']), toDecimal128OrNull('NULL', 0), toDecimal128OrNull(substring(arrayStringConcat(arrayMap(x -> leftPad(x, 8, '0'), arrayMap(x -> if(empty(x), '0', x), arrayResize(splitByChar('.', ''), 4)))), 8), 0))"
        },
        {
            "print parse_version('...')",
            "SELECT if((length(splitByChar('.', '...')) > 4) OR (length(splitByChar('.', '...')) < 1) OR (match('...', '.*[a-zA-Z]+.*') = 1) OR empty('...') OR hasAll(splitByChar('.', '...'), ['']), toDecimal128OrNull('NULL', 0), toDecimal128OrNull(substring(arrayStringConcat(arrayMap(x -> leftPad(x, 8, '0'), arrayMap(x -> if(empty(x), '0', x), arrayResize(splitByChar('.', '...'), 4)))), 8), 0))"
        },
        {
            "print parse_json( dynamic([1, 2, 3]))",
            "SELECT [1, 2, 3]"
        },
        {
            "print parse_json('{\"a\":123.5, \"b\":\"{\\\"c\\\":456}\"}')",
            "SELECT if(isValidJSON('{\"a\":123.5, \"b\":\"{\"c\":456}\"}'), JSON_QUERY('{\"a\":123.5, \"b\":\"{\"c\":456}\"}', '$'), toJSONString('{\"a\":123.5, \"b\":\"{\"c\":456}\"}'))"
        },
        {
            "print extract_json( '$.a' , '{\"a\":123, \"b\":\"{\"c\":456}\"}' , typeof(long))",
            "SELECT accurateCastOrNull(JSON_VALUE('{\"a\":123, \"b\":\"{\"c\":456}\"}', '$.a'), 'Int64')"
        },
        {
            "print bin(datetime(1970-05-11 13:45:07.456345672), 1ms)",
            "SELECT kql_bin(parseDateTime64BestEffortOrNull('1970-05-11 13:45:07.456345672', 9, 'UTC'), toIntervalNanosecond(1000000))"
        },
        {
            "print bin(datetime(1970-05-11 13:45:07.456345672), 1microseconds)",
            "SELECT kql_bin(parseDateTime64BestEffortOrNull('1970-05-11 13:45:07.456345672', 9, 'UTC'), toIntervalNanosecond(1000))"
        },
        {
            "print parse_command_line('echo \"hello world!\" print$?', 'windows')",
            "SELECT if(empty('echo \"hello world!\" print$?') OR hasAll(splitByChar(' ', 'echo \"hello world!\" print$?'), ['']), arrayMap(x -> NULL, splitByChar(' ', '')), splitByChar(' ', 'echo \"hello world!\" print$?'))"
        },
        {
            "print reverse(123)",
            "SELECT reverse(accurateCastOrNull(123, 'String'))"
        },
        {
            "print reverse(123.34)",
            "SELECT reverse(accurateCastOrNull(123.34, 'String'))"
        },
        {
            "print reverse('clickhouse')",
            "SELECT reverse(accurateCastOrNull('clickhouse', 'String'))"
        },
        {
            "print result=parse_csv('aa,b,cc')",
            "SELECT if(CAST(position('aa,b,cc', '\\n'), 'UInt8'), splitByChar(',', substring('aa,b,cc', 1, position('aa,b,cc', '\\n') - 1)), splitByChar(',', substring('aa,b,cc', 1, length('aa,b,cc')))) AS result"
        },
        {
            "print result_multi_record=parse_csv('record1,a,b,c\nrecord2,x,y,z')",
            "SELECT if(CAST(position('record1,a,b,c\\nrecord2,x,y,z', '\\n'), 'UInt8'), splitByChar(',', substring('record1,a,b,c\\nrecord2,x,y,z', 1, position('record1,a,b,c\\nrecord2,x,y,z', '\\n') - 1)), splitByChar(',', substring('record1,a,b,c\\nrecord2,x,y,z', 1, length('record1,a,b,c\\nrecord2,x,y,z')))) AS result_multi_record"
        },
        {
            "Customers | project name_abbr = strcat(substring(FirstName,0,3), ' ', substring(LastName,2))| order by LastName",
            "SELECT concat(ifNull(kql_tostring(if(toInt64(length(FirstName)) <= 0, '', substr(FirstName, (((0 % toInt64(length(FirstName))) + toInt64(length(FirstName))) % toInt64(length(FirstName))) + 1, 3))), ''), ifNull(kql_tostring(' '), ''), ifNull(kql_tostring(if(toInt64(length(LastName)) <= 0, '', substr(LastName, (((2 % toInt64(length(LastName))) + toInt64(length(LastName))) % toInt64(length(LastName))) + 1))), ''), '') AS name_abbr\nFROM Customers\nORDER BY LastName DESC"
        },
        {
            "print idx1 = indexof('abcdefg','cde')",
            "SELECT kql_indexof('abcdefg', 'cde', 0, -1, 1) AS idx1"
        },
        {
            "print idx2 = indexof('abcdefg','cde',0,3)",
            "SELECT kql_indexof('abcdefg', 'cde', 0, 3, 1) AS idx2"
        },
        {
            "print idx3 = indexof('abcdefg','cde',1,2)",
            "SELECT kql_indexof('abcdefg', 'cde', 1, 2, 1) AS idx3"
        },
        {
            "print idx5 = indexof('abcdefg','cde',-5) ",
            "SELECT kql_indexof('abcdefg', 'cde', -5, -1, 1) AS idx5"
        },
        {
            "print idx6 = indexof(1234567,5,1,4)   ",
            "SELECT kql_indexof(1234567, 5, 1, 4, 1) AS idx6"
        },
        {
            "print idx7 = indexof('abcdefg','cde',2,-1)",
            "SELECT kql_indexof('abcdefg', 'cde', 2, -1, 1) AS idx7"
        },
        {
            "print idx8 = indexof('abcdefgabcdefg', 'cde', 3)",
            "SELECT kql_indexof('abcdefgabcdefg', 'cde', 3, -1, 1) AS idx8"
        },
        {
            "print idx9 = indexof('abcdefgabcdefg', 'cde', 1, 13, 3) ",
            "SELECT kql_indexof('abcdefgabcdefg', 'cde', 1, 13, 3) AS idx9"
        }
})));
