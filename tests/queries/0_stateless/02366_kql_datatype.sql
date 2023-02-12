
DROP TABLE IF EXISTS tb1;
create table tb1 (    
str String
)ENGINE = Memory;
INSERT INTO tb1 VALUES ('123.561') , ('653.4');

-- datatable(s:string, i:long) [
--     '0', 0,
--     '1899', 1899,
--     '1900', 1900,
--     '2261', 2261,
--     '2262', 2262,
--     '10000', 10000
-- ]

drop table if exists datetime_test;
create table datetime_test(s String, i Int64) engine = Memory;
insert into datetime_test values ('0', 0), ('1899', 1899), ('1900', 1900), ('2261', 2261), ('2262', 2262), ('10000', 10000);

set dialect = 'kusto';
print '-- bool';
print bool(true);
print bool(null);
print bool('false'); -- { clientError BAD_ARGUMENTS }
print '-- int';
print int(123);
print int(null);
print int(-2147483648);
print int(2147483647);
print int('4'); -- { clientError BAD_ARGUMENTS }
print int(-2147483649); -- { serverError FUNCTION_THROW_IF_VALUE_IS_NON_ZERO }
print int(2147483648); -- { serverError FUNCTION_THROW_IF_VALUE_IS_NON_ZERO }
print '-- long';
print long(123);
print long(0xff);
print long(-1);
print long(null);
print long(-9223372036854775808);
print long(9223372036854775807);
print 456;
-- print long(-9223372036854775809); -- { serverError FUNCTION_THROW_IF_VALUE_IS_NON_ZERO }
print long(9223372036854775808); -- { serverError FUNCTION_THROW_IF_VALUE_IS_NON_ZERO }
print long('9023'); -- { clientError BAD_ARGUMENTS }
print '-- real';
print real(0.01);
print real(null);
print real(nan);
print real(+inf);
print real(-inf);
print double('4.2'); -- { clientError BAD_ARGUMENTS }
print '-- datetime';
print datetime(2015-12-31 23:59:59.9);
print datetime(2015-12-31);
print datetime('2014-05-25T08:20:03.123456');
print datetime('2014-11-08 15:55:55');
print datetime('2014-11-08 15:55');
print datetime('2014-11-08');
print datetime(null);
print datetime('2014-05-25T08:20:03.123456Z');
print datetime('2014-11-08 15:55:55.123456Z');
print datetime('2022') - datetime('2021');
print datetime('1970-05-11 13:45:07.456345672');
print '-- time';
print time(null);
print time(1.2:3:3);
print time(1.2:3:3.123);
print time(-1.2:3:3.123);
print time(001.02:03:03);
print time(001.02:03);
print time(02:03);
print time(02:03:04);
print time(02:03:04.5678901);
print time(24:03:04.5678901); -- { clientError BAD_ARGUMENTS }
print time(02:60:04.5678901); -- { clientError BAD_ARGUMENTS }
print time(02:03:60.5678901); -- { clientError BAD_ARGUMENTS }
print time(02:-03:04.5678901); -- { clientError BAD_ARGUMENTS }
print time(02:03:-04.5678901); -- { clientError BAD_ARGUMENTS }
print time(02:03:04.-5678901); -- { clientError BAD_ARGUMENTS }
print time(1.-02:03:04.5678901); -- { clientError BAD_ARGUMENTS }
print time(1.23); -- { clientError BAD_ARGUMENTS }
print time(02:03:04.56789012); -- { clientError BAD_ARGUMENTS }
print time(03:04.56789012); -- { clientError BAD_ARGUMENTS }
print time('14.02:03:04.12345');
print time('12:30:55.123');
print time(1d);
print time(-1d);
print time(6nanoseconds);
print time(6tick);
print time(2);
print time(2) + 1d;
print '-- timespan (time)';
print timespan(null);
print timespan(2d); --              2 days
--print timespan(1.5h); -- 	        1.5 hour
print timespan(30m); -- 	        30 minutes
print timespan(10s); -- 	        10 seconds
--print timespan(0.1s); -- 	        0.1 second
print timespan(100ms); -- 	        100 millisecond
print timespan(10microsecond); -- 	10 microseconds
print timespan(1tick); --           100 nanoseconds
--print timespan(1.5h) / timespan(30m);
print timespan('12.23:12:23') / timespan(1s);
print (timespan(1.5d) / timespan(0.6d)) * timespan(0.6d);
print tobool(timespan(0s));
print tobool(timespan(1d));
print todouble(timespan(1d));
-- print toint(timespan(1d)); -> 711573504
print tolong(timespan(1d));
print tostring(timespan(1d));
print tostring(timespan(2d) + timespan(4h) + timespan(8m) + timespan(16s) + timespan(123millis) + timespan(456micros) + timespan(789nanos));
print tostring((1h + 90d) * 2 + (6h + 32s + 30d + 2m) * 5);
print tostring(((1h + 90d) * 2 + (6h + 32s + 30d + 2m) * 5) / 2);
print tostring(-timespan(1d) - timespan(1h) - timespan(1m) - timespan(1s) - timespan(123456789nanos));
print todecimal(timespan(1d));
print 49h + (1h + 1m) * 999999h + 1s; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
print 1h * 1h; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
print 2h + 2; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
print 2h - 2; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
print '-- guid'
print guid(74be27de-1e4e-49d9-b579-fe0b331d3642);
print guid(null);
print '-- null';
print isnull(null);
print bool(null), int(null), long(null), real(null), double(null);
print '-- decimal';
print decimal(null);
print decimal(123.345);
print decimal(1e5);
print '-- dynamic'; -- no support for mixed types and bags for now
print dynamic(null);
print dynamic(1);
print dynamic(timespan(1d));
print dynamic([1,2,3]);
print dynamic([[1], [2], [3]]);
print dynamic(['a', "b", 'c']);
print '-- cast functions'
print '--tobool("true")'; -- == true
print tobool('true'); -- == true
print tobool('true') == toboolean('true'); -- == true
print '-- tobool("false")'; -- == false
print tobool('false'); -- == false
print tobool('false') == toboolean('false'); -- == false
print '-- tobool(1)'; -- == true
print tobool(1); -- == true
print tobool(1) == toboolean(1); -- == true
print '-- tobool(123)'; -- == true
print tobool(123); -- == true
print tobool(123) == toboolean(123); -- == true
print '-- tobool("abc")'; -- == null
print tobool('abc'); -- == null
print tobool('abc') == toboolean('abc'); -- == null
print '-- todouble()';
print todouble('123.4');
print todouble('abc') == null;
print '-- toreal()';
print toreal("123.4");
print toreal('abc') == null;
print '-- toint()';
print toint("123") == int(123);
print toint('abc');
print '-- tostring()';
print tostring(123);
print tostring(null);
print '-- todatetime()';
print todatetime("2015-12-24") == datetime(2015-12-24);
print isnull(todatetime('abc'));
print todatetime('1970-05-11 13:45:07.456345672') == datetime('1970-05-11 13:45:07.456345672');
print '-- totimespan()';
print totimespan(null);
print totimespan(1tick);
print totimespan('0.00:01:00');
print totimespan('abc');
print totimespan('12.23:12:23') / totimespan(1s);
print totimespan(strcat('12.', '23', ':12:', '23')) / timespan(1s);
print totimespan(timespan(16:30));
print totimespan("'asdadsasd");
print '-- tolong()';
print tolong('123');
print tolong('abc');
print '-- todecimal()';
print todecimal(123.345);
print todecimal(null);
print todecimal('abc');
print todecimal(1e5);
print todecimal(1e-5);
tb1 | project todecimal(str);
print todecimal('9999999999999999999999999999999999');
-- print todecimal(4 * 2 + 3); -> 11
