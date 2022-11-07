#include <Parsers/IParserBase.h>
#include <Parsers/Kusto/KustoFunctions/IParserKQLFunction.h>
#include <Parsers/Kusto/KustoFunctions/KQLCastingFunctions.h>
#include <Parsers/Kusto/KustoFunctions/KQLFunctionFactory.h>

#include <format>
#include <Poco/String.h>
#include<regex>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

bool ToBool::convertImpl(String & out, IParser::Pos & pos)
{
    const auto function_name = getKQLFunctionName(pos);
    if (function_name.empty())
        return false;

    const auto param = getArgument(function_name, pos);
    out = std::format(
        "multiIf(toString({0}) = 'true', true, "
        "toString({0}) = 'false', false, toInt64OrNull(toString({0})) != 0)",
        param,
        generateUniqueIdentifier());
    return true;
}

bool ToDateTime::convertImpl(String & out, IParser::Pos & pos)
{
    const auto function_name = getKQLFunctionName(pos);
    if (function_name.empty())
        return false;

    const auto param = getArgument(function_name, pos);

    out = std::format("parseDateTime64BestEffortOrNull(toString({0}),9,'UTC')", param);
    return true;
}

bool ToDouble::convertImpl(String & out, IParser::Pos & pos)
{
    const auto function_name = getKQLFunctionName(pos);
    if (function_name.empty())
        return false;

    const auto param = getArgument(function_name, pos);
    out = std::format("toFloat64OrNull(toString({0}))", param);
    return true;
}

bool ToInt::convertImpl(String & out, IParser::Pos & pos)
{
    const auto function_name = getKQLFunctionName(pos);
    if (function_name.empty())
        return false;

    const auto param = getArgument(function_name, pos);
    out = std::format("toInt32OrNull(toString({0}))", param);
    return true;
}

bool ToLong::convertImpl(String & out, IParser::Pos & pos)
{
    const auto function_name = getKQLFunctionName(pos);
    if (function_name.empty())
        return false;

    const auto param = getArgument(function_name, pos);
    out = std::format("toInt64OrNull(toString({0}))", param);
    return true;
}

bool ToString::convertImpl(String & out, IParser::Pos & pos)
{
    const auto function_name = getKQLFunctionName(pos);
    if (function_name.empty())
        return false;

    const auto param = getArgument(function_name, pos);
    out = std::format("ifNull(toString({0}), '')", param);
    return true;
} 
bool ToTimeSpan::convertImpl(String & out, IParser::Pos & pos)
{
    const auto function_name = getKQLFunctionName(pos);
    if (function_name.empty())
        return false;
    ++pos;
    String arg;
     if (pos->type == TokenType::QuotedIdentifier)
        arg = String(pos->begin + 1, pos->end - 1);
    else if (pos->type == TokenType::StringLiteral)
        arg = String(pos->begin, pos->end);
    else
        arg = getConvertedArgument(function_name, pos);
    
    if (pos->type == TokenType::StringLiteral || pos->type == TokenType::QuotedIdentifier)
    {
        ++pos;
        try
        {
           auto result =  kqlCallToExpression("time", {arg}, pos.max_depth);
            out = std::format("{}" , result);
        }
        catch(...)
        {
            out = "NULL";
        }
    }
    else
        out = std::format("{}" , arg);

    return true;
}

bool ToDecimal::convertImpl(String & out, IParser::Pos & pos)
{
    const String fn_name = getKQLFunctionName(pos);
    if (fn_name.empty())
        return false;
    ++pos;
    String res;
    if (pos->type == TokenType::QuotedIdentifier || pos->type == TokenType::StringLiteral || pos->type == TokenType::Number)     
    {
    --pos;
    res = getArgument(fn_name, pos);
    String scale = std::format("if(position({0}::String,'e') = 0 , ( countSubstrings({0}::String, '.') = 1 ? length(substr({0}::String, position({0}::String,'.') + 1)) : 0 ) , toUInt64(multiIf((position({0}::String,'e+') as x) >0 , substr({0}::String,x+2) ,  (position({0}::String,'e-')  as y )>0 , substr({0}::String,y+2)  ,  position({0}::String,'e-') = 0 AND position({0}::String,'e+') =0 AND position({0}::String,'e')>0, substr({0}::String,position({0}::String,'e')+1) , 0::String)))", res); 
    out = std::format("toTypeName({0}) = 'String' OR  toTypeName({0}) = 'FixedString' ? toDecimal128OrNull({0}::String , abs(34 - ({1}::UInt8))) : toDecimal128OrNull({0}::String , abs(17 - ({1}::UInt8)))", res, scale); 
    }
    else
    {
    --pos;
    res = getArgument(fn_name, pos);
    if( Poco::toUpper(res) == "NULL")
        out = "NULL";
    else
        out = std::format("toTypeName({0}) = 'String' OR  toTypeName({0}) = 'FixedString' ? toDecimal128OrNull({0}::String , 17) : toDecimal128OrNull({0}::String , 17)", res);
    }
    return true;
}

}
