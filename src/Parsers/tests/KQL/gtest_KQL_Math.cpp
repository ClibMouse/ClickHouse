#include <Parsers/tests/gtest_common.h>

#include <Parsers/Kusto/ParserKQLQuery.h>

INSTANTIATE_TEST_SUITE_P(ParserKQLQuery_Math, ParserTest,
    ::testing::Combine(
        ::testing::Values(std::make_shared<DB::ParserKQLQuery>()),
        ::testing::ValuesIn(std::initializer_list<ParserTestCase>{
        {
            "print abs(-5)",
            "SELECT abs(-5)"
        },
        {
            "print c1 = ceiling(-1.1), c2 = ceiling(0), c3 = ceiling(0.9)",
            "SELECT\n    ceil(-1.1) AS c1,\n    ceil(0) AS c2,\n    ceil(0.9) AS c3"
        },
        {
            "print exp(2);",
            "SELECT exp(2)"
        },
        {
            "print exp2(2)",
            "SELECT exp2(2)"
        },
        {
            "print exp10(3)",
            "SELECT exp10(3)"
        },
        {
            "print log(5)",
            "SELECT log(5)"
        },
        {
            "print log2(5)",
            "SELECT log2(5)"
        },
        {
            "print log10(5)",
            "SELECT log10(5)"
        },
        {
            "print pow(2, 3)",
            "SELECT pow(2, 3)"
        },
        {
            "print sqrt(256)",
            "SELECT sqrt(256)"
        },
        {
            "print acos(-0.45)",
            "SELECT acos(-0.45)"
        },
        {
            "print asin(0.5)",
            "SELECT asin(0.5)"
        },
        {
            "print atan(0.5);",
            "SELECT atan(0.5)"
        },
        {
            "print atan2(1, -1);",
            "SELECT atan2(1, -1)"
        },
        {
            "print cos(-0.45)",
            "SELECT cos(-0.45)"
        },
        {
            "print cot(-0.45)",
            "SELECT 1 / tan(-0.45)"
        },
	{
            "print degrees(pi()/4)",
            "SELECT degrees(pi() / 4)"
        },
        {
            "print gamma(-0.45)",
            "SELECT tgamma(-0.45)"
        },
        {
            "print isfinite(1.0/0.0)",
            "SELECT isFinite(1. / 0.)"
        },
        {
            "print isinf(1.0/0.0)",
            "SELECT isInfinite(1. / 0.)"
        },
        {
            "print loggamma(-0.45)",
            "SELECT lgamma(-0.45)"
        },
        {
            "print max_of(10, 1, -3, 17)",
            "SELECT arrayReduce('max', [10, 1, -3, 17])"
        },
        {
            "print min_of(10, 1, -3, 17)",
            "SELECT arrayReduce('min', [10, 1, -3, 17])"
        },
        {
            "print pi()",
            "SELECT pi()"
        },
        {
            "print radians(180)",
            "SELECT radians(180)"
        },
        {
            "print rand()",
            "SELECT if(0 < 2, randCanonical(), moduloOrZero(rand(), 0))"
        },
        {
            "print rand(1000)",
            "SELECT if(1000 < 2, randCanonical(), moduloOrZero(rand(), 1000))"
        },
        {
            "print rand(0)",
            "SELECT if(0 < 2, randCanonical(), moduloOrZero(rand(), 0))"
        },
	{
            "print round(2.15, 1)",
            "SELECT round(2.15, 1)"
        },
 	{
            "print sign(-42)",
            "SELECT sign(-42)"
        },
        {
            "print sin(-0.45)",
            "SELECT sin(-0.45)"
        },
        {
            "print tan(-0.45)",
            "SELECT tan(-0.45)"
        }
})));
