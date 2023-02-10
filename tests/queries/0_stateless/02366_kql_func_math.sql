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
