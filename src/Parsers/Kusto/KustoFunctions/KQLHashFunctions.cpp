#include "KQLHashFunctions.h"

#include <format>

namespace DB
{
bool Hash::convertImpl(String & out, IParser::Pos & pos)
{
    const auto fn_name = getKQLFunctionName(pos);

    if (fn_name.empty())
        return false;
    const auto arg = getArgument(fn_name, pos);
    const auto mod = getOptionalArgument(fn_name, pos);
    out = std::format(
        "{0}kql_hash({1} {2}",
        mod ? "if(" + mod.value() + " < 1, throwIf(true, 'hash(): argument 2 must be a constant positive long value'), toUInt64(" : "",
        arg,
        mod ? ")) % " + mod.value() + ")" : ")");
    return true;
}

bool HashSha256::convertImpl(String & out, IParser::Pos & pos)
{
    const auto function_name = getKQLFunctionName(pos);
    if (function_name.empty())
        return false;

    const auto arg = getArgument(function_name, pos, ArgumentState::Raw);
    out = "hex(SHA256(" + kqlCallToExpression("tostring", {arg}, pos.max_depth) + "))";

    return true;
}
}
