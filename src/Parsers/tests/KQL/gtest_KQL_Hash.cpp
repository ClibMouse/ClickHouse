#include <Parsers/tests/gtest_common.h>

#include <Parsers/Kusto/ParserKQLQuery.h>

INSTANTIATE_TEST_SUITE_P(ParserKQLQuery_Hash, ParserTest,
    ::testing::Combine(
        ::testing::Values(std::make_shared<DB::ParserKQLQuery>()),
        ::testing::ValuesIn(std::initializer_list<ParserTestCase>{
        {
            "print hash('World')",
            "SELECT kql_hash('World') AS print_0"
        },
        {
            "print hash('World', 100)",
            "SELECT kql_hash('World', 100) AS print_0"
        },
        {
            "print hash_sha256('World')",
            "SELECT lower(hex(SHA256(ifNull(kql_tostring('World'), '')))) AS print_0"
        },
})));
