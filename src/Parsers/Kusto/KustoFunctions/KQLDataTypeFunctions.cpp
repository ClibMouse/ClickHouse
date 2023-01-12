#include "KQLDataTypeFunctions.h"

#include <Parsers/Kusto/ParserKQLTimespan.h>
#include <Parsers/Kusto/Utilities.h>

#include <boost/lexical_cast/try_lexical_convert.hpp>
#include <Poco/String.h>

#include <format>
#include <regex>
#include <unordered_set>

namespace DB::ErrorCodes
{
extern const int BAD_ARGUMENTS;
extern const int SYNTAX_ERROR;
}

namespace
{
bool mapToAccurateCast(std::string & out, DB::IParser::Pos & pos, const std::string_view type_name)
{
    const auto function_name = DB::IParserKQLFunction::getKQLFunctionName(pos);
    if (function_name.empty())
        return false;

    ++pos;
    if (const auto & type = pos->type; type == DB::TokenType::QuotedIdentifier || type == DB::TokenType::StringLiteral)
        throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "String cannot be parsed as a literal of type {}", type_name);

    --pos;

    const auto arg = DB::IParserKQLFunction::getArgument(function_name, pos);
    out = std::format(
        "if(toTypeName({0}) = 'IntervalNanosecond' or isNull(accurateCastOrNull({0}, '{1}')) != isNull({0}), "
        "accurateCastOrNull(throwIf(true, 'Failed to parse {1} literal'), '{1}'), accurateCastOrNull({0}, '{1}'))",
        arg,
        type_name);

    return true;
}
}

namespace DB
{
bool DatatypeBool::convertImpl(String & out, IParser::Pos & pos)
{
    return mapToAccurateCast(out, pos, "Bool");
}

bool DatatypeDatetime::convertImpl(String & out, IParser::Pos & pos)
{
    const auto fn_name = getKQLFunctionName(pos);
    if (fn_name.empty())
        return false;

    const auto argument = extractLiteralArgumentWithoutQuotes(fn_name, pos);
    const auto mutated_argument = std::invoke(
        [&argument]
        {
            if (Int64 value; (boost::conversion::try_lexical_convert(argument, value) && (value < 1900 || value > 2261))
                || Poco::toLower(argument) == "null")
                return argument;

            return "'" + argument + "'";
        });

    out = std::format("kql_datetime({})", mutated_argument);
    return true;
}

bool DatatypeDynamic::convertImpl(String & out, IParser::Pos & pos)
{
    static const std::unordered_set<std::string_view> ALLOWED_FUNCTIONS{"date", "datetime", "dynamic", "time", "timespan"};

    const auto function_name = getKQLFunctionName(pos);
    if (function_name.empty())
        return false;

    ++pos;
    if (pos->type == TokenType::OpeningCurlyBrace)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Property bags are not supported for now in {}", function_name);

    while (!pos->isEnd() && pos->type != TokenType::ClosingRoundBracket)
    {
        if (const auto token_type = pos->type; token_type == TokenType::BareWord || token_type == TokenType::Number
            || token_type == TokenType::QuotedIdentifier || token_type == TokenType::StringLiteral)
        {
            if (const std::string_view token(pos->begin, pos->end); token_type == TokenType::BareWord && !ALLOWED_FUNCTIONS.contains(token))
            {
                ++pos;
                if (pos->type != TokenType::ClosingRoundBracket && pos->type != TokenType::ClosingSquareBracket
                    && pos->type != TokenType::Comma)
                    throw Exception(ErrorCodes::SYNTAX_ERROR, "Expression {} is not supported inside {}", token, function_name);

                --pos;
            }

            out.append(getConvertedArgument(function_name, pos));
        }
        else
        {
            out.append(pos->begin, pos->end);
            ++pos;
        }
    }

    return true;
}

bool DatatypeGuid::convertImpl(String & out, IParser::Pos & pos)
{
    const String fn_name = getKQLFunctionName(pos);
    if (fn_name.empty())
        return false;
    String guid_str;

    ++pos;
    if (pos->type == TokenType::QuotedIdentifier || pos->type == TokenType::StringLiteral)
        guid_str = String(pos->begin + 1, pos->end - 1);
    else
    {
        auto start = pos;
        while (!pos->isEnd() && pos->type != TokenType::PipeMark && pos->type != TokenType::Semicolon)
        {
            ++pos;
            if (pos->type == TokenType::ClosingRoundBracket)
                break;
        }
        --pos;
        guid_str = String(start->begin, pos->end);
    }
    out = std::format("toUUIDOrNull('{}')", guid_str);
    ++pos;
    return true;
}

bool DatatypeInt::convertImpl(String & out, IParser::Pos & pos)
{
    return mapToAccurateCast(out, pos, "Int32");
}

bool DatatypeLong::convertImpl(String & out, IParser::Pos & pos)
{
    return mapToAccurateCast(out, pos, "Int64");
}

bool DatatypeReal::convertImpl(String & out, IParser::Pos & pos)
{
    return mapToAccurateCast(out, pos, "Float64");
}

bool DatatypeTimespan::convertImpl(String & out, IParser::Pos & pos)
{
    const auto fn_name = getKQLFunctionName(pos);
    if (fn_name.empty())
        return false;

    const auto argument = extractLiteralArgumentWithoutQuotes(fn_name, pos);
    const auto ticks = ParserKQLTimespan::parse(argument);
    out = kqlTicksToInterval(ticks);

    return true;
}

bool DatatypeDecimal::convertImpl(String & out, IParser::Pos & pos)
{
    const String fn_name = getKQLFunctionName(pos);
    if (fn_name.empty())
        return false;

    ++pos;
    String arg;
    int scale = 0;
    int precision = 34;

    if (pos->type == TokenType::QuotedIdentifier || pos->type == TokenType::StringLiteral)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Failed to parse String as decimal Literal: {}", fn_name);

    --pos;
    arg = getArgument(fn_name, pos);

    //NULL expr returns NULL not exception
    static const std::regex expr{"^[0-9]+e[+-]?[0-9]+"};
    bool is_string = std::any_of(arg.begin(), arg.end(), ::isalpha) && Poco::toUpper(arg) != "NULL" && !(std::regex_match(arg, expr));
    if (is_string)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Failed to parse String as decimal Literal: {}", fn_name);

    if (std::regex_match(arg, expr))
    {
        auto exponential_pos = arg.find('e');
        if (arg[exponential_pos + 1] == '+' || arg[exponential_pos + 1] == '-')
            scale = std::stoi(arg.substr(exponential_pos + 2, arg.length()));
        else
            scale = std::stoi(arg.substr(exponential_pos + 1, arg.length()));

        out = std::format("toDecimal128({}::String,{})", arg, scale);
        return true;
    }

    if (const auto dot_pos = arg.find('.'); dot_pos != String::npos)
    {
        const auto length = static_cast<int>(std::ssize(arg.substr(0, dot_pos - 1)));
        scale = std::max(precision - length, 0);
    }
    if (is_string)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Failed to parse String as decimal Literal: {}", fn_name);

    if (scale < 0 || Poco::toUpper(arg) == "NULL")
        out = "NULL";
    else
        out = std::format("toDecimal128({}::String,{})", arg, scale);

    return true;
}
}
