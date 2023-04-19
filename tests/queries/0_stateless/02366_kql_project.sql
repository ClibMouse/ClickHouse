drop table if exists test;
create table test (s String) engine = Memory;
insert into test values ('127.0.0.1'), ('127.0.0.0');

set dialect = 'kusto';

print '-- #1 --';
print 1 | extend 2 | project ipv4_compare('127.0.0.1', '127.0.0.0'), 4, 5 | getschema | project ColumnName;

print '-- #2 --';
print parse_ipv4('1.2.3.4') | project 2 | extend 4 | project 3 | getschema | project ColumnName;

print '-- #3 --';
test | extend ipv4_compare(s, '127.0.0.0') | extend Column2 = 'c' | project ipv4_is_in_range(s, '127.1.2.3/8') | getschema | project ColumnName;
