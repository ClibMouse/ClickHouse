#include <Parsers/IParserBase.h>
#include <Parsers/ParserSetQuery.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/Kusto/ParserKQLQuery.h>
#include <Parsers/Kusto/ParserKQLStatement.h>
#include <Parsers/Kusto/KustoFunctions/IParserKQLFunction.h>
#include <Parsers/Kusto/KustoFunctions/KQLDateTimeFunctions.h>
#include <Parsers/Kusto/KustoFunctions/KQLStringFunctions.h>
#include <Parsers/Kusto/KustoFunctions/KQLDynamicFunctions.h>
#include <Parsers/Kusto/KustoFunctions/KQLCastingFunctions.h>
#include <Parsers/Kusto/KustoFunctions/KQLAggregationFunctions.h>
#include <Parsers/Kusto/KustoFunctions/KQLTimeSeriesFunctions.h>
#include <Parsers/Kusto/KustoFunctions/KQLIPFunctions.h>
#include <Parsers/Kusto/KustoFunctions/KQLBinaryFunctions.h>
#include <Parsers/Kusto/KustoFunctions/KQLGeneralFunctions.h>
#include <format>
#include <regex>
#include <optional>

namespace
{
bool mapToEndOfPeriod(std::string & out, DB::IParser::Pos & pos, const std::string_view period)
{
    const auto function_name = DB::IParserKQLFunction::getKQLFunctionName(pos);
    if (function_name.empty())
        return false;

    const auto datetime = DB::IParserKQLFunction::getArgument(function_name, pos, DB::IParserKQLFunction::ArgumentState::Raw);
    const auto offset = DB::IParserKQLFunction::getOptionalArgument(function_name, pos, DB::IParserKQLFunction::ArgumentState::Raw);
    out = std::format(
        "minus({}, {})",
        DB::IParserKQLFunction::kqlCallToExpression(
            std::format("startof{}", Poco::toLower(std::string(period))),
            {datetime, std::format("{} + 1", offset.value_or("0"))},
            pos.max_depth),
        DB::IParserKQLFunction::kqlCallToExpression("timespan", {"1tick"}, pos.max_depth));
    return true;
}

bool mapToStartOfPeriod(std::string & out, DB::IParser::Pos & pos, const std::string_view period)
{
    const auto function_name = DB::IParserKQLFunction::getKQLFunctionName(pos);
    if (function_name.empty())
        return false;

    const auto datetime = DB::IParserKQLFunction::getArgument(function_name, pos);
    const auto offset = DB::IParserKQLFunction::getOptionalArgument(function_name, pos);
    out = std::format("kql_todatetime(add{0}s(toStartOf{0}({1}), {2}))", period, datetime, offset.value_or("0"));
    return true;
}
}

namespace DB::ErrorCodes
{
extern const int BAD_ARGUMENTS;
extern const int SYNTAX_ERROR;
}

namespace DB
{
bool Ago::convertImpl(String & out, IParser::Pos & pos)
{
    const auto function_name = getKQLFunctionName(pos);
    if (function_name.empty())
        return false;

    const auto offset = getOptionalArgument(function_name, pos, ArgumentState::Raw);
    out = kqlCallToExpression(
        "now", {std::format("-1 * {}", offset.value_or(kqlCallToExpression("timespan", {"0"}, pos.max_depth)))}, pos.max_depth);
    return true;
}

bool DatetimeAdd::convertImpl(String & out, IParser::Pos & pos)
{
    const auto fn_name = getKQLFunctionName(pos);
    if (fn_name.empty())
        return false;

    auto period = getArgument(fn_name, pos);
    //remove quotes from period.
    trim(period);
    if (period.front() == '\"' || period.front() == '\'')
    {
        //period.remove
        period.erase(0, 1); // erase the first quote
        period.erase(period.size() - 1); // erase the last quote
    }

    const auto offset = getArgument(fn_name, pos);
    const auto datetime = getArgument(fn_name, pos);
    
    out = std::format("date_add({}, {}, {})",period,offset,datetime);

    return true;
};

bool DatetimePart::convertImpl(String & out, IParser::Pos & pos)
{
    const String fn_name = getKQLFunctionName(pos);
    if (fn_name.empty())
        return false;

    ++pos;
    String part = Poco::toUpper(getConvertedArgument(fn_name, pos));
    trim(part);
    if (part.front() == '\"' || part.front() == '\'')
    {
        //period.remove
        part.erase(0, 1); // erase the first quote
        part.erase(part.size() - 1); // erase the last quote
    }
    String date;
    if (pos->type == TokenType::Comma)
    {
        ++pos;
        date = getConvertedArgument(fn_name, pos);
    }
    String format;

    if (part == "YEAR")
        format = "%G";
    else if (part == "QUARTER")
        format = "%Q";
    else if (part == "MONTH")
        format = "%m";
    else if (part == "WEEK_OF_YEAR")
        format = "%V";
    else if (part == "DAY")
        format = "%e";
    else if (part == "DAYOFYEAR")
        format = "%j";
    else if (part == "HOUR")
        format = "%H";
    else if (part == "MINUTE")
        format = "%i";
    else if (part == "SECOND")
        format = "%S";
    else
        throw Exception(ErrorCodes::SYNTAX_ERROR, "Unexpected argument {} for {}", part, fn_name);

    out = std::format("formatDateTime({}, '{}')", date, format);
    return true;
}

bool DatetimeDiff::convertImpl(String & out, IParser::Pos & pos)
{
    const String fn_name = getKQLFunctionName(pos);
    if (fn_name.empty())
        return false;

    const auto period = getArgument(fn_name, pos);
    const auto datetime_lhs = getArgument(fn_name, pos);
    const auto datetime_rhs = getArgument(fn_name, pos);
    out = std::format("dateDiff({}, {}, {})", period, datetime_rhs, datetime_lhs);

    return true;
}

bool DayOfMonth::convertImpl(String & out, IParser::Pos & pos)
{
    return directMapping(out, pos, "toDayOfMonth");
}

bool DayOfWeek::convertImpl(String & out, IParser::Pos & pos)
{
    const auto fn_name = getKQLFunctionName(pos);
    if (fn_name.empty())
        return false;

    const auto datetime = getArgument(fn_name, pos);
    out = std::format("(toDayOfWeek({}) % 7) * {}", datetime, kqlCallToExpression("timespan", {"1d"}, pos.max_depth));

    return true;
}

bool DayOfYear::convertImpl(String & out, IParser::Pos & pos)
{
    return directMapping(out, pos, "toDayOfYear");
}

bool EndOfMonth::convertImpl(String & out, IParser::Pos & pos)
{
    return mapToEndOfPeriod(out, pos, "Month");
}

bool EndOfDay::convertImpl(String & out, IParser::Pos & pos)
{
    return mapToEndOfPeriod(out, pos, "Day");
}

bool EndOfWeek::convertImpl(String & out, IParser::Pos & pos)
{
    return mapToEndOfPeriod(out, pos, "Week");
}

bool EndOfYear::convertImpl(String & out, IParser::Pos & pos)
{
    return mapToEndOfPeriod(out, pos, "Year");
}

bool FormatDateTime::convertImpl(String & out, IParser::Pos & pos)
{
    const String fn_name = getKQLFunctionName(pos);
    if (fn_name.empty())
        return false;
    String formatspecifier;
    ++pos;
    const auto datetime = getConvertedArgument(fn_name, pos);
    ++pos;
    auto format = getConvertedArgument(fn_name, pos);
    trim(format);

    //remove quotes and end space from format argument.
    if (format.front() == '\"' || format.front() == '\'')
    {
        format.erase(0, 1); // erase the first quote
        format.erase(format.size() - 1); // erase the last quote
    }

    std::vector<String> res;
    getTokens(format, res);
    std::string::size_type i = 0;
    size_t decimal = 0;
    while (i < format.size())
    {
        char c = format[i];
        if (!isalpha(c))
        {
            //delimiter
            if (c == ' ' || c == '-' || c == '_' || c == '[' || c == ']' || c == '/' || c == ',' || c == '.' || c == ':')
                formatspecifier = formatspecifier + c;
            else
                throw Exception(ErrorCodes::SYNTAX_ERROR, "Invalid format delimiter in function: {}", fn_name);
            ++i;
        }
        else
        {
            //format specifier
            String arg = res.back();

            if (arg == "y" || arg == "yy")
                formatspecifier = formatspecifier + "%y";
            else if (arg == "yyyy")
                formatspecifier = formatspecifier + "%Y";
            else if (arg == "M" || arg == "MM")
                formatspecifier = formatspecifier + "%m";
            else if (arg == "s" || arg == "ss")
                formatspecifier = formatspecifier + "%S";
            else if (arg == "m" || arg == "mm")
                formatspecifier = formatspecifier + "%i";
            else if (arg == "h" || arg == "hh")
                formatspecifier = formatspecifier + "%h";
            else if (arg == "H" || arg == "HH")
                formatspecifier = formatspecifier + "%H";
            else if (arg == "d")
                formatspecifier = formatspecifier + "%e";
            else if (arg == "dd")
                formatspecifier = formatspecifier + "%d";
            else if (arg == "tt")
                formatspecifier = formatspecifier + "%p";
            else if (arg.starts_with('f') || arg.starts_with('F'))
                decimal = arg.size();
            else
                throw Exception(ErrorCodes::SYNTAX_ERROR, "Format specifier {} in function: {} is not supported", arg, fn_name);
            res.pop_back();
            i = i + arg.size();
        }
    }
    if (decimal > 0 && formatspecifier.find('.') != String::npos)
    {
        out = std::format(
            "concat("
            "substring(toString(formatDateTime({0} , '{1}')),1, position(toString(formatDateTime({0},'{1}')),'.')) ,"
            "substring(substring(toString({0}), position(toString({0}),'.')+1),1,{2}),"
            "substring(toString(formatDateTime({0},'{1}')), position(toString(formatDateTime({0},'{1}')), '.')+1, length "
            "(toString(formatDateTime({0},'{1}'))))) ",
            datetime,
            formatspecifier,
            decimal);
    }
    else
        out = std::format("formatDateTime({0},'{1}')", datetime, formatspecifier);

    return true;
}

bool FormatTimeSpan::convertImpl(String & out, IParser::Pos & pos)
{
    static const std::unordered_set<char> ALLOWED_DELIMITERS{' ', '/', '-', ':', ',', '.', '_', '[', ']'};
    static const std::unordered_map<char, std::tuple<std::string_view, std::optional<int>, bool, int, std::optional<std::string_view>>>
        ATTRIBUTES_BY_FORMAT_CHARACTER{
            {'d', {"1d", std::nullopt, false, 8, "leftPad"}},
            {'f', {"1tick", 10'000'000, true, 7, "rightPad"}},
            {'F', {"1tick", 10'000'000, true, 7, std::nullopt}},
            {'h', {"1h", 24, false, 2, "leftPad"}},
            {'H', {"1h", 24, false, 2, "leftPad"}},
            {'m', {"1m", 60, false, 2, "leftPad"}},
            {'s', {"1s", 60, false, 2, "leftPad"}}};

    const auto fn_name = getKQLFunctionName(pos);
    if (fn_name.empty())
        return false;

    const auto timespan = getArgument(fn_name, pos);
    const auto format = getArgument(fn_name, pos);
    if (std::ssize(format) < 3 || format.front() != format.back() || format.front() != '\'')
        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Expected non-empty string literal as the second argument to {}", fn_name);

    std::string current_streak;
    std::string delimited_parts;
    const auto convert_streak = [&current_streak, &timespan, &delimited_parts, &pos]
    {
        while (!current_streak.empty())
        {
            if (!delimited_parts.empty())
                delimited_parts.append(", ");

            const auto attributes_it = ATTRIBUTES_BY_FORMAT_CHARACTER.find(current_streak.front());
            if (attributes_it == ATTRIBUTES_BY_FORMAT_CHARACTER.cend())
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected format character: {}", current_streak.front());

            const auto & [timespan_unit, modulus, should_truncate, max_length, pad_function] = attributes_it->second;
            const auto streak_length = std::ssize(current_streak);
            const auto part_length = std::min(streak_length, static_cast<std::ptrdiff_t>(max_length));
            current_streak.erase(current_streak.cbegin(), current_streak.cbegin() + part_length);

            auto expression = std::format("intDiv({}, {})", timespan, kqlCallToExpression("timespan", {timespan_unit}, pos.max_depth));
            expression = std::format("toString({})", modulus ? std::format("modulo({}, {})", expression, *modulus) : expression);
            if (should_truncate)
                expression = std::format("substring({}, 1, {})", expression, part_length);

            delimited_parts.append(
                pad_function ? std::format("if(length({1}) < {2}, {0}({1}, {2}, '0'), {1})", *pad_function, expression, part_length)
                             : expression);
        }
    };

    for (const auto & c : std::string_view(format.cbegin() + 1, format.cend() - 1))
    {
        if (ALLOWED_DELIMITERS.contains(c))
        {
            convert_streak();
            delimited_parts.append(std::format(", '{}'", c));
        }
        else if (ATTRIBUTES_BY_FORMAT_CHARACTER.contains(c))
        {
            if (!current_streak.empty() && current_streak.back() != c)
                convert_streak();

            current_streak.push_back(c);
        }
        else
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unexpected character '{}' in format string of {}", c, fn_name);
    }

    convert_streak();
    out = "concat(" + delimited_parts + ", '')";
    return true;
}

bool GetMonth::convertImpl(String & out, IParser::Pos & pos)
{
    return directMapping(out, pos, "toMonth");
}

bool GetYear::convertImpl(String & out, IParser::Pos & pos)
{
    return directMapping(out, pos, "toYear");
}

bool HoursOfDay::convertImpl(String & out, IParser::Pos & pos)
{
    return directMapping(out, pos, "toHour");
}

bool MakeTimeSpan::convertImpl(String & out, IParser::Pos & pos)
{
     const String fn_name = getKQLFunctionName(pos);
     if (fn_name.empty())
         return false;

    const auto arg1 = getArgument(fn_name, pos);
    const auto arg2 = getArgument(fn_name, pos);
    const auto arg3 = getOptionalArgument(fn_name, pos);
    const auto arg4 = getOptionalArgument(fn_name, pos);

    const auto & [day, hour, minute, second]
        = std::invoke([&arg1, &arg2, &arg3, &arg4]
                      { return arg4 ? std::make_tuple(arg1, arg2, *arg3, *arg4) : std::make_tuple("0", arg1, arg2, arg3.value_or("0")); });

    out = std::format(
        "{} * {} + {} * {} + {} * {} + {} * {}",
        day,
        kqlCallToExpression("timespan", {"1d"}, pos.max_depth),
        hour,
        kqlCallToExpression("timespan", {"1h"}, pos.max_depth),
        minute,
        kqlCallToExpression("timespan", {"1m"}, pos.max_depth),
        second,
        kqlCallToExpression("timespan", {"1s"}, pos.max_depth));

    return true;
}

bool MakeDateTime::convertImpl(String & out, IParser::Pos & pos)
{
    const auto fn_name = getKQLFunctionName(pos);
    if (fn_name.empty())
        return false;

    const auto year = getArgument(fn_name, pos);
    const auto month = getArgument(fn_name, pos);
    const auto day = getArgument(fn_name, pos);
    const auto hour = getOptionalArgument(fn_name, pos);
    const auto minute = getOptionalArgument(fn_name, pos);
    const auto second = getOptionalArgument(fn_name, pos);
    out = std::format(
        "if({0} between 1900 and 2261 and {1} between 1 and 12 and {3} between 0 and 59 and {4} between 0 and 59 and {5} >= 0 and {5} < 60 "
        " and isNotNull(toModifiedJulianDayOrNull(concat(leftPad(toString({0}), 4, '0'), '-', leftPad(toString({1}), 2, '0'), '-', leftPad(toString({2}), 2, '0')))), "
        "toDateTime64OrNull(toString(makeDateTime64({0}, {1}, {2}, {3}, {4}, truncate({5}), ({5} - truncate({5})) * 1e7, 7, 'UTC')), 9), null)",
        year,
        month,
        day,
        hour.value_or("0"),
        minute.value_or("0"),
        second.value_or("0"));

    return true;
}

bool Now::convertImpl(String & out, IParser::Pos & pos)
{
    const auto fn_name = getKQLFunctionName(pos);
    if (fn_name.empty())
        return false;

    const auto offset = getOptionalArgument(fn_name, pos);
    out = "now64(9, 'UTC')" + (offset ? " + " + *offset : "");

    return true;
}

bool StartOfDay::convertImpl(String & out, IParser::Pos & pos)
{
    return mapToStartOfPeriod(out, pos, "Day");
}

bool StartOfMonth::convertImpl(String & out, IParser::Pos & pos)
{
    return mapToStartOfPeriod(out, pos, "Month");
}

bool StartOfWeek::convertImpl(String & out, IParser::Pos & pos)
{
    return mapToStartOfPeriod(out, pos, "Week");
}

bool StartOfYear::convertImpl(String & out, IParser::Pos & pos)
{
    return mapToStartOfPeriod(out, pos, "Year");
}

bool UnixTimeMicrosecondsToDateTime::convertImpl(String & out, IParser::Pos & pos)
{
    const auto fn_name = getKQLFunctionName(pos);
    if (fn_name.empty())
        return false;

    const auto value = getArgument(fn_name, pos);
    out = std::format("kql_todatetime(fromUnixTimestamp64Micro({}, 'UTC'))", value);

    return true;
}

bool UnixTimeMillisecondsToDateTime::convertImpl(String & out, IParser::Pos & pos)
{
    const auto fn_name = getKQLFunctionName(pos);
    if (fn_name.empty())
        return false;

    const auto value = getArgument(fn_name, pos);    
    out = std::format("kql_todatetime(fromUnixTimestamp64Milli({}, 'UTC'))", value);

    return true;
}

bool UnixTimeNanosecondsToDateTime::convertImpl(String & out, IParser::Pos & pos)
{
    const auto fn_name = getKQLFunctionName(pos);
    if (fn_name.empty())
        return false;

    const auto value = getArgument(fn_name, pos);     
    out = std::format("kql_todatetime(fromUnixTimestamp64Nano({}, 'UTC'))", value);

    return true;
}

bool UnixTimeSecondsToDateTime::convertImpl(String & out, IParser::Pos & pos)
{
    const String fn_name = getKQLFunctionName(pos);
    if (fn_name.empty())
        return false;

    ++pos;
    if (pos->type == TokenType::QuotedIdentifier || pos->type == TokenType::StringLiteral)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "{} accepts only long, int and double type of arguments", fn_name);

    const auto expression = getConvertedArgument(fn_name, pos);
    out = std::format(
        "if(toTypeName(assumeNotNull({0})) in ['Int32', 'Int64', 'Float64', 'UInt32', 'UInt64'], "
        "kql_todatetime({0}), kql_todatetime(throwIf(true, '{1} only accepts int, long and double type of arguments')))",
        expression,
        fn_name);

    return true;
}

bool WeekOfYear::convertImpl(String & out, IParser::Pos & pos)
{
    const String fn_name = getKQLFunctionName(pos);
    if (fn_name.empty())
        return false;
    ++pos;
    const String time_str = getConvertedArgument(fn_name, pos);
    out = std::format("toWeek({},3,'UTC')", time_str);
    return true;
}

bool MonthOfYear::convertImpl(String & out, IParser::Pos & pos)
{
    return directMapping(out, pos, "toMonth");
}

}
