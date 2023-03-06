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
