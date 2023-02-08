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
        }
})));
