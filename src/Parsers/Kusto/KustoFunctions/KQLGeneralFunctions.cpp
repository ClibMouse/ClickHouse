#include "KQLGeneralFunctions.h"

#include <format>

namespace DB
{
bool Bin::convertImpl(String & out,IParser::Pos & pos) { return directMapping(out, pos, "kql_bin"); }

bool BinAt::convertImpl(String & out, IParser::Pos & pos)
{
    const auto fn_name = getKQLFunctionName(pos);
    if (fn_name.empty())
        return false;

    const auto value = getArgument(fn_name, pos);
    const auto round_to = getArgument(fn_name, pos);
    const auto fixed_point = getArgument(fn_name, pos);
    out = std::format("kql_bin({0} - {2}, {1}) + {2}", value, round_to, fixed_point);

    return true;
}

bool Case::convertImpl(String & out,IParser::Pos & pos)
{
    return directMapping(out, pos, "multiIf");
}
}
