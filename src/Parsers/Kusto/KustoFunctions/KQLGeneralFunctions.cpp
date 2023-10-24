#include "KQLGeneralFunctions.h"

#include <IO/WriteBufferFromString.h>
#include <Parsers/IAST.h>
#include <Parsers/Kusto/ParserKQLPrint.h>
#include <Parsers/Kusto/ParserKQLStatement.h>

#include <format>

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

bool LookupContains::convertImpl(String & out, IParser::Pos & pos)
{
    return directMapping(out, pos, "dictHas");
}

bool Lookup::convertImpl(String & out, IParser::Pos & pos)
{
    auto temp_pos = pos;
    const String fn_name = getKQLFunctionName(temp_pos);

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
        ++num_of_args;
        if (temp_pos->type == TokenType::ClosingRoundBracket)
            break;
        ++temp_pos;
    }
    if (num_of_args == 3)
        return directMapping(out, pos, "dictGet");
    else if (num_of_args == 4)
        return directMapping(out, pos, "dictGetOrDefault");
    else
        throw Exception(ErrorCodes::SYNTAX_ERROR, "number of arguments do not match in function: {}", fn_name);
}

bool GetType::convertImpl(String & out, IParser::Pos & pos)
{
    return directMapping(out, pos, "kql_gettype");
}

bool ToScalar::convertImpl(String & out, IParser::Pos & pos)
{
    const auto function_name = getKQLFunctionName(pos);
    if (function_name.empty())
        return false;

    Expected expected;
    ASTPtr subquery;
    try
    {
        subquery = std::make_shared<ASTSelectQuery>();
        if (!ParserKQLPrint().parse(pos, subquery, expected))
            subquery.reset();
    }
    catch (...)
    {
        subquery.reset();
    }

    if (KQLContext kql_context; !subquery && !ParserKQLTableFunction(kql_context).parse(pos, subquery, expected))
        return false;

    --pos;
    WriteBufferFromOwnString write_buffer;
    subquery->format(IAST::FormatSettings(write_buffer, true));

    out = std::format("(select tuple(*) from ({}) limit 1).1", write_buffer.stringView());
    return true;
}

bool Not::convertImpl(String & out, IParser::Pos & pos)
{
    return directMapping(out, pos, "kql_not");
}

}
