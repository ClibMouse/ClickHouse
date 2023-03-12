set dialect = 'kusto';

print '-- range function int1 < int2, default int3 --';
print range(1, 10);

print '-- range function int1 < int2, int3 > 0 --';
print range(1, 10, 2);

print '-- range function int1 < int2, int3 == 0 --';
print range(1, 10, 0);

print '-- range function int1 < int2, int3 < 0 --';
print range(1, 10, -2);

print '-- range function -int1 < int2, int3 > 0 --';
print range(-1, 10, 2);

print '-- range function int1 > int2 default int3--';
print range(17, 10);

print '-- range function int1 > int2, int3 > 0 --';
print range(17, 10, 2);

print '-- range function int1 > int2, int3 == 0 --';
print range(17, 10, 0);

print '-- range function int1 > int2, int3 < 0 --';
print range(17, 10, -2);

print '-- range function int1 > -int2, int3 < 0 --';
print range(17, -10, -2);

print '-- range function int1 == int2 default int3--';
print range(10, 10);

print '-- range function int1 == int2, int3 > 0 --';
print range(10, 10, 2);

print '-- range function int1 == int2, int3 == 0 --';
print range(10, 10, 0);

print '-- range function int1 == int2, int3 < 0 --';
print range(10, 10, -2);

print '-- range function int1 == int2, float3 > 0 --';
print range(10, 10, 2.2);

print '-- range function float1 < float2, default step --';
print range(1.2, 10.3);

print '-- range function float1 < float2, float3 > 0--';
print range(1.2, 10.3, 2.2);

print '-- range function  float1 < float2, float3 == 0 --';
print range(1.2, 10.3, 0);

print '-- range function  float1 < float2, float3 < 0 --';
print range(1.2, 10.3, -2.2);

print '-- range function float1 > float2, default step --';
print range(21.2, 10.3);

print '-- range function float1 > float2, float3 > 0--';
print range(21.2, 10.3, 2.2);

print '-- range function  float1 > float2, float3 == 0 --';
print range(21.2, 10.3, 0);

print '-- range function  float1 > float2, float3 < 0 --';
print range(21.2, 10.3, -2.2);

print '-- range function float1 == float2, default step --';
print range(21.2, 21.2);

print '-- range function float1 == float2, float3 > 0--';
print range(21.2, 21.2, 2.2);

print '-- range function  float1 == float2, float3 == 0 --';
print range(21.2, 21.2, 0);

print '-- range function  float1 == float2, float3 < 0 --';
print range(21.2, 21.2, -2.2);

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

print '-- range function -timespan, timespan, timespan --';
print range(-1h, 5h, 2h);

print '-- range function timespan, timespan --';
print range(1h, 5h);

print '-- range function timespan1 > timespan2, negative timespan3 < 0 --';
print range(11h, 5h, -2h);

print '-- range function float timespan1 < timespan2, timespan3 > 0 --';
print range(1.5h, 5h, 2h);

print '-- range function timespan1 < timespan2, timespan3 = 0 --';
print range(1.5h, 5h, 0h);

print '-- range function timespan1 == timespan2, timespan3 = 0 --';
print range(5h, 5h, 2h);

print '-- range function datetime1 < datetime2 , timespan > 0 --';
print range(endofday(datetime(2017-01-01 10:10:17)), endofday(datetime(2017-01-03 10:10:17)), 1d);

print '-- range function datetime1 > datetime2 , timespan > 0 --';
print range(endofday(datetime(2017-01-05 10:10:17)), endofday(datetime(2017-01-03 10:10:17)), 1d);

print '-- range function datetime1 > datetime2 , timespan < 0 --';
print range(endofday(datetime(2017-01-05 10:10:17)), endofday(datetime(2017-01-03 10:10:17)), -1d);

print '-- range orerator int, int, int --';
range Age from 20 to 25 step 1;

print '-- range orerator float, float, float --';
range temp from 20.5 to 25.5 step 1.5;

print '-- range orerator datetime, datetime, timespan --';
range FirstWeek from datetime('2023-01-01') to datetime('2023-01-07') step 1d;