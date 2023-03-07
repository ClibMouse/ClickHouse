#include "KQLCastingFunctions.h"

#include <format>

namespace DB
{
bool ToBool::convertImpl(String & out, IParser::Pos & pos)
{
    const auto function_name = getKQLFunctionName(pos);
    if (function_name.empty())
        return false;

    const auto param = getArgument(function_name, pos);
    out = std::format(
        "multiIf(toString({0}) = 'true', true, "
        "toString({0}) = 'false', false, toInt64OrNull(toString({0})) != 0)",
        param);
    return true;
}

bool ToDateTime::convertImpl(String & out, IParser::Pos & pos)
{
    return directMapping(out, pos, "kql_todatetime");
}

bool ToDouble::convertImpl(String & out, IParser::Pos & pos)
{
    const auto function_name = getKQLFunctionName(pos);
    if (function_name.empty())
        return false;

    const auto param = getArgument(function_name, pos);
    out = std::format("toFloat64OrNull(toString({0})) / if(toTypeName({0}) = 'IntervalNanosecond', 100, 1)", param);
    return true;
}

bool ToInt::convertImpl(String & out, IParser::Pos & pos)
{
    const auto function_name = getKQLFunctionName(pos);
    if (function_name.empty())
        return false;

    const auto param = getArgument(function_name, pos);
    out = std::format("intDiv(toInt32OrNull(toString({0})), if(toTypeName({0}) = 'IntervalNanosecond', 100, 1))", param);
    return true;
}

bool ToLong::convertImpl(String & out, IParser::Pos & pos)
{
    const auto function_name = getKQLFunctionName(pos);
    if (function_name.empty())
        return false;

    const auto param = getArgument(function_name, pos);
    out = std::format("intDiv(toInt64OrNull(toString({0})), if(toTypeName({0}) = 'IntervalNanosecond', 100, 1))", param);
    return true;
}

bool ToString::convertImpl(String & out, IParser::Pos & pos)
{
    const auto function_name = getKQLFunctionName(pos);
    if (function_name.empty())
        return false;

    const auto argument = getArgument(function_name, pos);
    out = std::format("ifNull(kql_tostring({}), '')", argument);
    return true;
}

bool ToTimeSpan::convertImpl(String & out, IParser::Pos & pos)
{
    return directMapping(out, pos, "kql_totimespan");
}

bool ToDecimal::convertImpl(String & out, IParser::Pos & pos)
{
    const auto fn_name = getKQLFunctionName(pos);
    if (fn_name.empty())
        return false;

    ++pos;
    if (pos->type == TokenType::QuotedIdentifier || pos->type == TokenType::StringLiteral || pos->type == TokenType::Number)
    {
        --pos;
        const auto arg = getArgument(fn_name, pos);
        const auto scale = std::format("if (position({0}::String,'e') = 0,(countSubstrings({0}::String,'.') = 1 ? length(substr({0}::String, position({0}::String,'.') + 1)): 0), toUInt64(multiIf ((position({0}::String,'e+') as x) > 0, substr({0}::String, x + 2),(position({0}::String, 'e-') as y) > 0, substr({0}::String, y + 2), position({0}::String, 'e-') = 0 AND position({0}::String, 'e+') =0 AND position({0}::String, 'e') > 0,substr({0}::String, position({0}::String, 'e') + 1), 0::String)))",arg);
        out = std::format("toTypeName({0}) = 'String' OR toTypeName({0}) = 'FixedString' ? toDecimal128OrNull({0}::String , ({1}::UInt8)) : toDecimal128OrNull({0}::String , ({1}::UInt8))", arg, scale);
    }
    else
    {
        --pos;
        const auto arg = getArgument(fn_name, pos);
        out = std::format("toDecimal128OrNull({0}::Nullable(String), 17) / if(toTypeName({0}) = 'IntervalNanosecond', 100, 1)", arg);
    }

    return true;
}
}
