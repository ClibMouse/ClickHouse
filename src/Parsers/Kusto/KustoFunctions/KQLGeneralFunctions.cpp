#include "KQLGeneralFunctions.h"

namespace DB::ErrorCodes
{
extern const int SYNTAX_ERROR;
}

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

bool Lookup::convertImpl(String & out, IParser::Pos & pos)
{
    auto temp_pos = pos;
    String fn_name = getKQLFunctionName(temp_pos);

    if (fn_name.empty())
        return false;
    int num_of_args = 0;
    temp_pos = pos;
    ++temp_pos;
    ++temp_pos;
    
    String arg;

    while (!temp_pos->isEnd() && temp_pos->type != TokenType::PipeMark && temp_pos->type != TokenType::Semicolon)
    {
        arg = getConvertedArgument(fn_name, temp_pos);
        num_of_args++;
        ++temp_pos;
    }
    if (num_of_args == 3)
        return directMapping(out, pos, "dictGet");
    else if (num_of_args == 4)
        return directMapping(out, pos, "dictGetOrDefault");
    else
        throw Exception("number of arguments do not match in function: " + fn_name, ErrorCodes::SYNTAX_ERROR);
}

}
