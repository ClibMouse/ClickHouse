#include <Parsers/tests/gtest_common.h>

#include <Parsers/Kusto/ParserKQLStatement.h>

INSTANTIATE_TEST_SUITE_P(ParserKQLQuery_String, ParserTest,
    ::testing::Combine(
        ::testing::Values(std::make_shared<DB::ParserKQLStatement>()),
        ::testing::ValuesIn(std::initializer_list<ParserTestCase>{
        {
            "print base64_encode_fromguid(A)",
            "SELECT if(toTypeName(A) NOT IN ['UUID', 'Nullable(UUID)'], toString(throwIf(true, 'Expected guid as argument')), base64Encode(UUIDStringToNum(toString(A), 2))) AS print_0"
        },
        {
            "print base64_decode_toguid(A)",
            "SELECT toUUIDOrNull(UUIDNumToString(toFixedString(base64Decode(A), 16), 2)) AS print_0"
        },
        {
            "print base64_decode_toarray('S3VzdG8=')",
            "SELECT IF((length('S3VzdG8=') % 4) != 0, [NULL], IF(length(tryBase64Decode('S3VzdG8=')) = 0, [NULL], IF(countMatches(substring('S3VzdG8=', 1, length('S3VzdG8=') - 2), '=') > 0, [NULL], arrayMap(x -> reinterpretAsUInt8(x), splitByRegexp('', base64Decode(assumeNotNull(IF(length(tryBase64Decode('S3VzdG8=')) = 0, '', 'S3VzdG8=')))))))) AS print_0"
        },
        {
            "print res = base64_decode_tostring('S3VzdG8====')",
            "SELECT IF((length('S3VzdG8====') % 4) != 0, NULL, IF(countMatches(substring('S3VzdG8====', 1, length('S3VzdG8====') - 2), '=') > 0, NULL, tryBase64Decode('S3VzdG8===='))) AS res"
        },
        {
            "print replace_regex('Hello, World!', '.', '\\0\\0')",
            "SELECT replaceRegexpAll('Hello, World!', '.', '\\0\\0') AS print_0"
        },
        {
            "print has_any_index('this is an example', dynamic(['this', 'example'])) ",
            "SELECT if(empty(['this', 'example']), -1, indexOf(arrayMap(x -> (x IN splitByChar(' ', 'this is an example')), if(empty(['this', 'example']), [''], arrayMap(x -> toString(x), ['this', 'example']))), 1) - 1) AS print_0"
        },
        {
            "print has_any_index('this is an example', dynamic([]))",
            "SELECT if(empty([]), -1, indexOf(arrayMap(x -> (x IN splitByChar(' ', 'this is an example')), if(empty([]), [''], arrayMap(x -> toString(x), []))), 1) - 1) AS print_0"
        },
        {
            "print translate('krasp', 'otsku', 'spark')",
            "SELECT if(length('otsku') = 0, '', translate('spark', 'krasp', multiIf(length('otsku') = 0, 'krasp', (length('krasp') - length('otsku')) > 0, concat('otsku', repeat(substr('otsku', length('otsku'), 1), toUInt16(length('krasp') - length('otsku')))), (length('krasp') - length('otsku')) < 0, substr('otsku', 1, length('krasp')), 'otsku'))) AS print_0"
        },
        {
            "print trim_start('[^\\w]+', strcat('-  ','Te st1','// $'))",
            "SELECT replaceRegexpOne(concat(ifNull(kql_tostring('-  '), ''), ifNull(kql_tostring('Te st1'), ''), ifNull(kql_tostring('// $'), ''), ''), concat('^', '[^\\\\w]+'), '') AS print_0"
        },
        {
            "print trim_end('.com', 'bing.com')",
            "SELECT replaceRegexpOne('bing.com', concat('.com', '$'), '') AS print_0"
        },
        {
            "print trim('--', '--https://bing.com--')",
            "SELECT replaceRegexpOne(replaceRegexpOne('--https://bing.com--', concat('--', '$'), ''), concat('^', '--'), '') AS print_0"
        },
        {
            "print bool(1)",
            "SELECT if((toTypeName(1) = 'IntervalNanosecond') OR ((accurateCastOrNull(1, 'Bool') IS NULL) != (1 IS NULL)), accurateCastOrNull(throwIf(true, 'Failed to parse Bool literal'), 'Bool'), accurateCastOrNull(1, 'Bool')) AS print_0"
        },
        {
            "print guid(74be27de-1e4e-49d9-b579-fe0b331d3642)",
            "SELECT toUUIDOrNull('74be27de-1e4e-49d9-b579-fe0b331d3642') AS print_0"
        },
        {
            "print guid('74be27de-1e4e-49d9-b579-fe0b331d3642')",
            "SELECT toUUIDOrNull('74be27de-1e4e-49d9-b579-fe0b331d3642') AS print_0"
        },
        {
            "print guid('74be27de1e4e49d9b579fe0b331d3642')",
            "SELECT toUUIDOrNull('74be27de1e4e49d9b579fe0b331d3642') AS print_0"
        },
        {
            "print int(32.5)",
            "SELECT if((toTypeName(32.5) = 'IntervalNanosecond') OR ((accurateCastOrNull(32.5, 'Int32') IS NULL) != (32.5 IS NULL)), accurateCastOrNull(throwIf(true, 'Failed to parse Int32 literal'), 'Int32'), accurateCastOrNull(32.5, 'Int32')) AS print_0"
        },
        {
            "print long(32.5)",
            "SELECT if((toTypeName(32.5) = 'IntervalNanosecond') OR ((accurateCastOrNull(32.5, 'Int64') IS NULL) != (32.5 IS NULL)), accurateCastOrNull(throwIf(true, 'Failed to parse Int64 literal'), 'Int64'), accurateCastOrNull(32.5, 'Int64')) AS print_0"
        },
        {
            "print real(32.5)",
            "SELECT if((toTypeName(32.5) = 'IntervalNanosecond') OR ((accurateCastOrNull(32.5, 'Float64') IS NULL) != (32.5 IS NULL)), accurateCastOrNull(throwIf(true, 'Failed to parse Float64 literal'), 'Float64'), accurateCastOrNull(32.5, 'Float64')) AS print_0"
        },
        {
            "print time('1.22:34:8.128')",
            "SELECT toIntervalNanosecond(167648128000000) AS print_0"
        },
        {
            "print time('1d')",
            "SELECT toIntervalNanosecond(86400000000000) AS print_0"
        },
        {
            "print time('1.5d')",
            "SELECT toIntervalNanosecond(129600000000000) AS print_0"
        },
        {
            "print timespan('1.5d')",
            "SELECT toIntervalNanosecond(129600000000000) AS print_0"
        },
        {
            "print extract('x=([0-9.]+)', 1, 'hello x=456|wo' , typeof(bool));",
            "SELECT accurateCastOrNull(toInt64OrNull(kql_extract('hello x=456|wo', 'x=([0-9.]+)', 1)), 'Boolean') AS print_0"
        },
        {
            "print extract('x=([0-9.]+)', 1, 'hello x=456|wo' , typeof(date));",
            "SELECT accurateCastOrNull(kql_extract('hello x=456|wo', 'x=([0-9.]+)', 1), 'DateTime') AS print_0"
        },
        {
            "print extract('x=([0-9.]+)', 1, 'hello x=456|wo' , typeof(guid));",
            "SELECT accurateCastOrNull(kql_extract('hello x=456|wo', 'x=([0-9.]+)', 1), 'UUID') AS print_0"
        },
        {
            "print extract('x=([0-9.]+)', 1, 'hello x=456|wo' , typeof(int));",
            "SELECT accurateCastOrNull(kql_extract('hello x=456|wo', 'x=([0-9.]+)', 1), 'Int32') AS print_0"
        },
        {
            "print extract('x=([0-9.]+)', 1, 'hello x=456|wo' , typeof(long));",
            "SELECT accurateCastOrNull(kql_extract('hello x=456|wo', 'x=([0-9.]+)', 1), 'Int64') AS print_0"
        },
        {
            "print extract('x=([0-9.]+)', 1, 'hello x=456|wo' , typeof(real));",
            "SELECT accurateCastOrNull(kql_extract('hello x=456|wo', 'x=([0-9.]+)', 1), 'Float64') AS print_0"
        },
        {
            "print extract('x=([0-9.]+)', 1, 'hello x=456|wo' , typeof(decimal));",
            "SELECT toDecimal128OrNull(if(countSubstrings(kql_extract('hello x=456|wo', 'x=([0-9.]+)', 1), '.') > 1, NULL, kql_extract('hello x=456|wo', 'x=([0-9.]+)', 1)), length(substr(kql_extract('hello x=456|wo', 'x=([0-9.]+)', 1), position(kql_extract('hello x=456|wo', 'x=([0-9.]+)', 1), '.') + 1))) AS print_0"
        },
        {
            "print parse_version('1.2.3.40')",
            "SELECT if((length(splitByChar('.', '1.2.3.40')) > 4) OR (length(splitByChar('.', '1.2.3.40')) < 1) OR (match('1.2.3.40', '.*[a-zA-Z]+.*') = 1) OR empty('1.2.3.40') OR hasAll(splitByChar('.', '1.2.3.40'), ['']), toDecimal128OrNull('NULL', 0), toDecimal128OrNull(substring(arrayStringConcat(arrayMap(x -> leftPad(x, 8, '0'), arrayMap(x -> if(empty(x), '0', x), arrayResize(splitByChar('.', '1.2.3.40'), 4)))), 8), 0)) AS print_0"
        },
        {
            "print parse_version('1')",
            "SELECT if((length(splitByChar('.', '1')) > 4) OR (length(splitByChar('.', '1')) < 1) OR (match('1', '.*[a-zA-Z]+.*') = 1) OR empty('1') OR hasAll(splitByChar('.', '1'), ['']), toDecimal128OrNull('NULL', 0), toDecimal128OrNull(substring(arrayStringConcat(arrayMap(x -> leftPad(x, 8, '0'), arrayMap(x -> if(empty(x), '0', x), arrayResize(splitByChar('.', '1'), 4)))), 8), 0)) AS print_0"
        },
        {
            "print parse_version('')",
            "SELECT if((length(splitByChar('.', '')) > 4) OR (length(splitByChar('.', '')) < 1) OR (match('', '.*[a-zA-Z]+.*') = 1) OR empty('') OR hasAll(splitByChar('.', ''), ['']), toDecimal128OrNull('NULL', 0), toDecimal128OrNull(substring(arrayStringConcat(arrayMap(x -> leftPad(x, 8, '0'), arrayMap(x -> if(empty(x), '0', x), arrayResize(splitByChar('.', ''), 4)))), 8), 0)) AS print_0"
        },
        {
            "print parse_version('...')",
            "SELECT if((length(splitByChar('.', '...')) > 4) OR (length(splitByChar('.', '...')) < 1) OR (match('...', '.*[a-zA-Z]+.*') = 1) OR empty('...') OR hasAll(splitByChar('.', '...'), ['']), toDecimal128OrNull('NULL', 0), toDecimal128OrNull(substring(arrayStringConcat(arrayMap(x -> leftPad(x, 8, '0'), arrayMap(x -> if(empty(x), '0', x), arrayResize(splitByChar('.', '...'), 4)))), 8), 0)) AS print_0"
        },
        {
            "print parse_json( dynamic([1, 2, 3]))",
            "SELECT [1, 2, 3] AS print_0"
        },
        {
            "print parse_json('{\"a\":123.5, \"b\":\"{\\\"c\\\":456}\"}')",
            "SELECT if(isValidJSON('{\"a\":123.5, \"b\":\"{\"c\":456}\"}'), JSON_QUERY('{\"a\":123.5, \"b\":\"{\"c\":456}\"}', '$'), toJSONString('{\"a\":123.5, \"b\":\"{\"c\":456}\"}')) AS print_0"
        },
        {
            "print extract_json( '$.a' , '{\"a\":123, \"b\":\"{\"c\":456}\"}' , typeof(long))",
            "SELECT accurateCastOrNull(JSON_VALUE('{\"a\":123, \"b\":\"{\"c\":456}\"}', '$.a'), 'Int64') AS print_0"
        },
        {
            "print extract_json( '$.a' , '{\"a\":123, \"b\":\"{\"c\":456}\"}' , typeof(bool))",
            "SELECT if(toInt64OrNull(JSON_VALUE('{\"a\":123, \"b\":\"{\"c\":456}\"}', '$.a')) > 0, true, false) AS print_0"
        },
        {
            "print parse_command_line('echo \"hello world!\" print$?', 'windows')",
            "SELECT if(empty('echo \"hello world!\" print$?') OR hasAll(splitByChar(' ', 'echo \"hello world!\" print$?'), ['']), arrayMap(x -> NULL, splitByChar(' ', '')), splitByChar(' ', 'echo \"hello world!\" print$?')) AS print_0"
        },
        {
            "print reverse(123)",
            "SELECT reverse(ifNull(kql_tostring(123), '')) AS print_0"
        },
        {
            "print reverse(123.34)",
            "SELECT reverse(ifNull(kql_tostring(123.34), '')) AS print_0"
        },
        {
            "print reverse('clickhouse')",
            "SELECT reverse(ifNull(kql_tostring('clickhouse'), '')) AS print_0"
        },
        {
            "print parse_csv('aa,b,cc')",
            "SELECT if(CAST(position('aa,b,cc', '\\n'), 'UInt8'), splitByChar(',', substring('aa,b,cc', 1, position('aa,b,cc', '\\n') - 1)), splitByChar(',', substring('aa,b,cc', 1, length('aa,b,cc')))) AS print_0"
        },
        {
            "print parse_csv('record1,a,b,c\nrecord2,x,y,z')",
            "SELECT if(CAST(position('record1,a,b,c\\nrecord2,x,y,z', '\\n'), 'UInt8'), splitByChar(',', substring('record1,a,b,c\\nrecord2,x,y,z', 1, position('record1,a,b,c\\nrecord2,x,y,z', '\\n') - 1)), splitByChar(',', substring('record1,a,b,c\\nrecord2,x,y,z', 1, length('record1,a,b,c\\nrecord2,x,y,z')))) AS print_0"
        },
        {
            "Customers | project name_abbr = strcat(substring(FirstName,0,3), ' ', substring(LastName,2))| order by LastName",
            "SELECT concat(ifNull(kql_tostring(if(toInt64(length(FirstName)) <= 0, '', substr(FirstName, (((0 % toInt64(length(FirstName))) + toInt64(length(FirstName))) % toInt64(length(FirstName))) + 1, 3))), ''), ifNull(kql_tostring(' '), ''), ifNull(kql_tostring(if(toInt64(length(LastName)) <= 0, '', substr(LastName, (((2 % toInt64(length(LastName))) + toInt64(length(LastName))) % toInt64(length(LastName))) + 1))), ''), '') AS name_abbr\nFROM Customers\nORDER BY LastName DESC NULLS LAST"
        },
        {
            "print indexof('abcdefg','cde')",
            "SELECT kql_indexof(kql_tostring('abcdefg'), kql_tostring('cde'), 0, -1, 1) AS print_0"
        },
        {
            "print indexof('abcdefg','cde',0,3)",
            "SELECT kql_indexof(kql_tostring('abcdefg'), kql_tostring('cde'), 0, 3, 1) AS print_0"
        },
        {
            "print indexof('abcdefg','cde',1,2)",
            "SELECT kql_indexof(kql_tostring('abcdefg'), kql_tostring('cde'), 1, 2, 1) AS print_0"
        },
        {
            "print indexof('abcdefg','cde',-5)",
            "SELECT kql_indexof(kql_tostring('abcdefg'), kql_tostring('cde'), -5, -1, 1) AS print_0"
        },
        {
            "print indexof(1234567,5,1,4)",
            "SELECT kql_indexof(kql_tostring(1234567), kql_tostring(5), 1, 4, 1) AS print_0"
        },
        {
            "print indexof('abcdefg','cde',2,-1)",
            "SELECT kql_indexof(kql_tostring('abcdefg'), kql_tostring('cde'), 2, -1, 1) AS print_0"
        },
        {
            "print indexof('abcdefgabcdefg', 'cde', 3)",
            "SELECT kql_indexof(kql_tostring('abcdefgabcdefg'), kql_tostring('cde'), 3, -1, 1) AS print_0"
        },
        {
            "print indexof('abcdefgabcdefg', 'cde', 1, 13, 3) ",
            "SELECT kql_indexof(kql_tostring('abcdefgabcdefg'), kql_tostring('cde'), 1, 13, 3) AS print_0"
        },
        {
            "print indexof(1d, '.')",
            "SELECT kql_indexof(kql_tostring(toIntervalNanosecond(86400000000000)), kql_tostring('.'), 0, -1, 1) AS print_0"
        },
        {
            "print strrep(3s,2,' ')",
            "SELECT substr(repeat(concat(ifNull(kql_tostring(toIntervalNanosecond(3000000000)), ''), ' '), 2), 1, length(repeat(concat(ifNull(kql_tostring(toIntervalNanosecond(3000000000)), ''), ' '), 2)) - length(' ')) AS print_0"
        },
        {
            "print isempty(1.12345)",
            "SELECT empty(ifNull(kql_tostring(1.12345), '')) AS print_0"
        },
        {
            "print isnotempty('1.12345')",
            "SELECT notEmpty(ifNull(kql_tostring('1.12345'), '')) AS print_0"
        },
        {
            "print string_size('⒦⒰⒮⒯⒪')",
            "SELECT length('⒦⒰⒮⒯⒪') AS print_0"
        },
        {
            "print to_utf8('⒦⒰⒮⒯⒪')",
            "SELECT arrayMap(x -> if(substring(bin(x), 1, 1) = '0', reinterpretAsInt64(reverse(UNBIN(substring(bin(x), 2, 7)))), if(substring(bin(x), 1, 3) = '110', reinterpretAsInt64(reverse(UNBIN(concat(substring(bin(x), 4, 5), substring(bin(x), 11, 6))))), if(substring(bin(x), 1, 4) = '1110', reinterpretAsInt64(reverse(UNBIN(concat(substring(bin(x), 5, 4), substring(bin(x), 11, 6), substring(bin(x), 19, 6))))), if(substring(bin(x), 1, 5) = '11110', reinterpretAsInt64(reverse(UNBIN(concat(substring(bin(x), 6, 3), substring(bin(x), 11, 6), substring(bin(x), 19, 6), substring(bin(x), 27, 6))))), -1)))), ngrams('⒦⒰⒮⒯⒪', 1)) AS print_0"
        },
        {
            "print new_guid()",
            "SELECT generateUUIDv4() AS print_0"
	},
        {
            "print parse_url('https://john:123@google.com:1234/this/is/a/path?k1=v1&k2=v2#fragment')",
            "SELECT kql_parseurl('https://john:123@google.com:1234/this/is/a/path?k1=v1&k2=v2#fragment') AS print_0",
        }
})));
