-- datatable (a:dynamic, s:string, i:long) []

drop table if exists test;
create table test (a Array(String), s String, i Int64) engine = Memory;

select '-- #1 --';
select * from getschema(select * from test);

select '-- #2 --';
select * from getschema(select *, length(s) as x from test);

select '-- #3 --';
select ColumnName, ColumnType from getschema(select * from test) where DataType like '%String%';

select '-- #4 --';
select * from getschema(select * from getschema(select 1));

set dialect = 'kusto';
print '-- #5 --';
test | getschema;

print '-- #6 --';
test | extend x = strlen(s) | getschema;

print '-- #7 --';
test | getschema | where DataType like '%String%' | project ColumnName, ColumnType;

print '-- #8 --';
print 1 | getschema | getschema;

print '-- #9 --';
test | project a, s | getschema;

print '-- #10 --';
test | summarize avg(i) | getschema;

print '-- #11 --';
test | take 1000 | extend x = strlen(s) | top 500 by x asc | where x > 0 or i > 0 | distinct s, i, x | top-hitters 100 of s by i | getschema;

print '-- #12 --';
test | project a, i | count | getschema;

print '-- #13 --';
test | top-nested 50 of s with others = "Others" by sum(i) | getschema;

print '-- #14 --';
range x from 0d to 365d step 1d | extend l = tolong(x) | lookup kind = inner (test) on $left.l == $right.i | mv-expand a | getschema;
