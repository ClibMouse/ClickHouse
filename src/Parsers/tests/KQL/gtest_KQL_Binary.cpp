#include <Parsers/tests/gtest_common.h>

#include <Parsers/Kusto/ParserKQLStatement.h>

INSTANTIATE_TEST_SUITE_P(ParserKQLQuery_Binary, ParserKQLTest,
    ::testing::Combine(
        ::testing::Values(std::make_shared<DB::ParserKQLStatement>()),
        ::testing::ValuesIn(std::initializer_list<ParserTestCase>{
        {
            "print binary_and(A, B)",
            "SELECT bitAnd(CAST(A, 'Int64'), CAST(B, 'Int64')) AS print_0"
        },
        {
            "print binary_not(A)",
            "SELECT bitNot(CAST(A, 'Int64')) AS print_0"
        },
        {
            "print binary_or(A, B)",
            "SELECT bitOr(CAST(A, 'Int64'), CAST(B, 'Int64')) AS print_0"
        },
        {
            "print binary_shift_left(A, B)",
            "SELECT if(B < 0, NULL, bitShiftLeft(CAST(A, 'Int64'), B)) AS print_0"
        },
        {
            "print binary_shift_right(A, B)",
            "SELECT if(B < 0, NULL, bitShiftRight(CAST(A, 'Int64'), B)) AS print_0"
        },
        {
            "print binary_xor(A, B)",
            "SELECT bitXor(CAST(A, 'Int64'), CAST(B, 'Int64')) AS print_0"
        },
        {
            "print bitset_count_ones(A)",
            "SELECT bitCount(A) AS print_0"
        }
})));
