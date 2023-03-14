#include <Parsers/tests/gtest_common.h>

#include <Parsers/Kusto/ParserKQLQuery.h>

INSTANTIATE_TEST_SUITE_P(ParserKQLQuery_Hash, ParserTest,
    ::testing::Combine(
        ::testing::Values(std::make_shared<DB::ParserKQLQuery>()),
        ::testing::ValuesIn(std::initializer_list<ParserTestCase>{
        {
            "print hash('World')",
            "SELECT xxHash64KQL('World')"
        },
        {
            "print hash('World', 100)",
            "SELECT if(100 < 1, throwIf(true, 'hash(): argument 2 must be a constant positive long value'), toUInt64(xxHash64KQL('World')) % 100)"
        },
        {
            "print hash_sha256('World')",
            "SELECT hex(SHA256(ifNull(kql_tostring('World'), '')))"
        },
})));
