set dialect = 'kusto';
print '-- isnan --';
print isnan(double(nan));
print isnan(4.2);
print isnan(4); -- { serverError FUNCTION_THROW_IF_VALUE_IS_NON_ZERO }
print isnan(real(+inf));
print isnan(dynamic(null)); -- { serverError FUNCTION_THROW_IF_VALUE_IS_NON_ZERO }
print '-- abs --';
print abs(-5);
print '-- ceiling --';
print ceiling(-1.1);
print ceiling(0);
print ceiling(0.9);
print '-- exp --';
print exp(2);
print '-- exp2 --';
print exp2(2);
print '-- exp10 --';
print exp10(3);
print '-- log --';
print log(5);
print '-- log2 --';
print log2(5);
print '-- log10 --';
print log10(5);
print '-- pow --';
print pow(2, 3);
print '-- sqrt --';
print sqrt(256);
