set dialect = 'kusto';

print '-- range function int, int, int --';
print range(1, 10, 2);

print '-- range function int, int --';
print range(1, 10);

print '-- range function float, float, float --';
print range(1.2, 10.3, 2.2);

print '-- range function postive float, float, int --';
print range(1.2, 10.3, 2);

print '-- range function postive float, int, float --';
print range(1.2, 10, 2.2);

print '-- range function postive integer, int, float --';
print range(1, 10, 2.2);

print '-- range function postive intger, float, float --';
print range(1, 10.5, 2.2);

print '-- range function postive float, int, int --';
print range(1.2, 10, 2);

print '-- range function postive int, int, negative int --';
print range(12, 3, -2);

print '-- range function postive float, int, negative float --';
print range(12.8, 3, -2.3);

print '-- range function datetime, datetime, timespan --';
print range(datetime('2001-01-01'), datetime('2001-01-02'), 5h);

print '-- range function datetime, datetime, negative timespan --';
print range(datetime('2001-01-03'), datetime('2001-01-02'), -5h);

print '-- range function datetime, datetime --';
print range(datetime('2001-01-01'), datetime('2001-01-02'));

print '-- range function timespan, timespan, timespan --';
print range(1h, 5h, 2h);

print '-- range function timespan, timespan --';
print range(1h, 5h);

print '-- range function timespan, timespan, negative timespan --';
print range(11h, 5h, -2h);

print '-- range function float timespan, timespan, timespan --';
print range(1.5h, 5h, 2h);

print '-- range function endofday, endofday, timespan --';
print range(endofday(datetime(2017-01-01 10:10:17)), endofday(datetime(2017-01-03 10:10:17)), 1d);