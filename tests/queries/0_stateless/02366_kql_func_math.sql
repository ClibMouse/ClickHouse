set dialect = 'kusto';
print '-- isnan --';
print isnan(double(nan));
print isnan(4.2);
print isnan(4); -- { serverError FUNCTION_THROW_IF_VALUE_IS_NON_ZERO }
print isnan(real(+inf));
print isnan(dynamic(null)); -- { serverError FUNCTION_THROW_IF_VALUE_IS_NON_ZERO }
print '-- abs --';
print abs(-5);
print abs('test'); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
print abs(1d); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
print '-- ceiling --';
print ceiling(-1.1);
print ceiling(0);
print ceiling(0.9);
print ceiling('test'); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
print '-- exp --';
print exp(2);
print exp(0.5);
print exp(-1);
print exp('test'); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
print '-- exp2 --';
print exp2(2);
print exp2(0.5);
print exp2(-1);
print exp2('test'); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
print '-- exp10 --';
print exp10(3);
print exp10(0.5);
print exp10(-3);
print exp10('test'); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
print '-- log --';
print log(5);
print log(0.5);
print log(-5);
print log('test'); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
print '-- log2 --';
print log2(5);
print log2(0.5);
print log2(-5);
print log2('test'); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
print '-- log10 --';
print log10(5);
print log10(0.5);
print log10(-5);
print log10('test'); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
print '-- pow --';
print pow(2, 3);
print pow(0.5, 0.5);
print pow(-1, -1);
print pow('test', 'test'); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
print '-- sqrt --';
print sqrt(256);
print sqrt(-1);
print sqrt(0.5);
print sqrt('test'); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
print '-- acos --';
print acos(1);
print acos(-0.45);
print acos('test'); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
print '-- asin --';
print asin(1);
print asin(0.5);
print asin('test'); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
print '-- atan --';
print atan(1);
print atan(0.5);
print atan('test'); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
print '-- atan2 --';
print atan2(1, -1);
print atan2(-0.5, 0.5);
print atan2('test', 'test'); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
print '-- cos --';
print cos(1);
print cos(-0.45);
print cos('test'); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
print '-- cot --';
print cot(1);
print cot(-0.45);
print cot(0);
print cot('test'); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
print '-- degrees --';
print degrees(pi()/4);
print degrees('test'); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
print '-- gamma --';
print gamma(1);
print gamma(-0.45);
print gamma('test'); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
print '-- isfinite --';
print isfinite(1.0/0.0);
print isfinite('test'); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
print '-- isinf --';
print isinf(1.0/0.0);
print isinf('test'); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
print '-- loggamma --';
print loggamma(5);
print loggamma(-0.45);
print loggamma('test'); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
print '-- max_of --';
print max_of(10, 1, -3, 17);
print max_of('test', 'abc');
print max_of(1); -- { clientError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
print max_of(1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1); -- { clientError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
print '-- min_of --';
print min_of(10, 1, -3, 17);
print min_of('test', 'abc');
print min_of(1); -- { clientError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
print min_of(1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1); -- { clientError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
print '-- pi --';
print pi();
print pi('any'); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
print '-- radians --';
print radians(90);
print radians(180);
print radians(360);
print radians('test'); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
print '-- rand --';
print x = rand() | project x >= 0 and x <= 1;
print x = rand(1234) | project x >= 0 and x <= 1233;
print '-- round --';
print round(2.15, 1);
print round('test'); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
print '-- sign --';
print sign(-42);
print sign(0);
print sign(11.2);
print sign('test'); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
print '-- sin --';
print sin(1);
print sin(-0.45);
print sin('test'); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
print '-- tan --';
print tan(1);
print tan(-0.45);
print tan('test'); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
