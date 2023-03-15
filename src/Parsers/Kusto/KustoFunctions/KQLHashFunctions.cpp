#include "KQLHashFunctions.h"

#include <format>

namespace DB
{
bool Hash::convertImpl(String & out, IParser::Pos & pos)
{
    return directMapping(out, pos, "kql_hash");
}

bool HashSha256::convertImpl(String & out, IParser::Pos & pos)
{
    const auto function_name = getKQLFunctionName(pos);
    if (function_name.empty())
        return false;

    const auto arg = getArgument(function_name, pos, ArgumentState::Raw);
    out = "lower(hex(SHA256(" + kqlCallToExpression("tostring", {arg}, pos.max_depth) + ")))";

    return true;
}
}
