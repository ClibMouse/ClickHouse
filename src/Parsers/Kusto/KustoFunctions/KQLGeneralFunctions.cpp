#include "KQLGeneralFunctions.h"

namespace DB
{
bool Bin::convertImpl(String & out, IParser::Pos & pos)
{
    return directMapping(out, pos, "kql_bin");
}

bool BinAt::convertImpl(String & out, IParser::Pos & pos)
{
    return directMapping(out, pos, "kql_bin_at");
}

bool Case::convertImpl(String & out, IParser::Pos & pos)
{
    return directMapping(out, pos, "multiIf");
}

bool Iff::convertImpl(String & out, IParser::Pos & pos)
{
    return directMapping(out, pos, "If");
}

bool Iif::convertImpl(String & out, IParser::Pos & pos)
{
    return directMapping(out, pos, "If");
}
}
