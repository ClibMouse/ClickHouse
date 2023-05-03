#include "KQLAggregationFunctions.h"

#include <Common/StringUtils/StringUtils.h>

#include <format>
#include <numeric>

namespace DB::ErrorCodes
{
extern const int NOT_IMPLEMENTED;
extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

namespace
{
void checkAccuracy(const std::optional<std::string> & accuracy)
{
    if (accuracy && *accuracy != "4")
        throw DB::Exception(DB::ErrorCodes::NOT_IMPLEMENTED, "only accuracy of 4 is supported");
}
}

namespace DB
{
bool ArgMax::convertImpl(String & out, IParser::Pos & pos)
{
    auto arg_count = 0;
    String fn_name = getKQLFunctionName(pos);

    if (fn_name.empty())
        return false;
    ++pos;
    ++arg_count;
    String expr_to_maximize = getConvertedArgument(fn_name, pos);
    while (pos->type == TokenType::Comma)
    {
        ++pos;
        ++arg_count;
        const auto expr_to_return = getConvertedArgument(fn_name, pos);
        if (expr_to_return == expr_to_maximize)
        {
            continue;
        }
        out += std::format("argMax({}, {}) as {},", expr_to_return, expr_to_maximize, expr_to_return);
    }
    if (arg_count < 2)
    {
        throw DB::Exception(DB::ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "{} requires at least two arguments", fn_name);
    }
    out += std::format("argMax({}, {})", expr_to_maximize, expr_to_maximize);
    return true;
}

bool ArgMin::convertImpl(String & out, IParser::Pos & pos)
{
    auto arg_count = 0;
    String fn_name = getKQLFunctionName(pos);

    if (fn_name.empty())
        return false;
    ++pos;
    ++arg_count;
    String expr_to_maximize = getConvertedArgument(fn_name, pos);
    while (pos->type == TokenType::Comma)
    {
        ++pos;
        ++arg_count;
        const auto expr_to_return = getConvertedArgument(fn_name, pos);
        if (expr_to_return == expr_to_maximize)
        {
            continue;
        }
        out += std::format("argMin({}, {}) as {},", expr_to_return, expr_to_maximize, expr_to_return);
    }
    if (arg_count < 2)
    {
        throw DB::Exception(DB::ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "{} requires at least two arguments", fn_name);
    }
    out += std::format("argMin({}, {})", expr_to_maximize, expr_to_maximize);
    return true;
}

bool Avg::convertImpl(String & out, IParser::Pos & pos)
{
    return directMapping(out, pos, "avg");
}

bool AvgIf::convertImpl(String & out, IParser::Pos & pos)
{
    return directMapping(out, pos, "avgIf");
}

bool BinaryAllAnd::convertImpl(String & out, IParser::Pos & pos)
{
    return directMapping(out, pos, "groupBitAnd");
}

bool BinaryAllOr::convertImpl(String & out, IParser::Pos & pos)
{
    return directMapping(out, pos, "groupBitOr");
}

bool BinaryAllXor::convertImpl(String & out, IParser::Pos & pos)
{
    return directMapping(out, pos, "groupBitXor");
}

bool BuildSchema::convertImpl([[maybe_unused]] String & out, [[maybe_unused]] IParser::Pos & pos)
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "{} is not yet implemented", getName());
}

bool Count::convertImpl(String & out, IParser::Pos & pos)
{
    return directMapping(out, pos, "count");
}

bool CountIf::convertImpl(String & out, IParser::Pos & pos)
{
    return directMapping(out, pos, "countIf");
}

bool DCount::convertImpl(String & out, IParser::Pos & pos)
{
    String fn_name = getKQLFunctionName(pos);

    if (fn_name.empty())
        return false;
    ++pos;
    String value = getConvertedArgument(fn_name, pos);
    if (pos->type == TokenType::Comma)
    {
        ++pos;
        const auto accuracy = getConvertedArgument(fn_name, pos);
        out = "count(DISTINCT " + value + " , " + accuracy + ")";
    }
    else
        out = "count(DISTINCT " + value + ")";
    return true;
}

bool DCountIf::convertImpl(String & out, IParser::Pos & pos)
{
    String fn_name = getKQLFunctionName(pos);

    if (fn_name.empty())
        return false;
    ++pos;
    String value = getConvertedArgument(fn_name, pos);
    ++pos;
    String condition = getConvertedArgument(fn_name, pos);
    if (pos->type == TokenType::Comma)
    {
        ++pos;
        const auto accuracy = getConvertedArgument(fn_name, pos);
        out = "count(DISTINCT " + value + " , " + condition + " , " + accuracy + ")";
    }
    else
        out = "countIf(DISTINCT " + value + " , " + condition + ")";
    return true;
}

bool DCountHll::convertImpl(String & out, IParser::Pos & pos)
{
    const auto fn_name = getKQLFunctionName(pos);
    if (fn_name.empty())
        return false;

    const auto expr = getArgument(fn_name, pos);
    out = std::format("uniqCombined64Merge(18)({})", expr);

    return true;
}

bool Hll::convertImpl(String & out, IParser::Pos & pos)
{
    const auto fn_name = getKQLFunctionName(pos);
    if (fn_name.empty())
        return false;

    const auto expr = getArgument(fn_name, pos);
    const auto accuracy = getOptionalArgument(fn_name, pos);

    checkAccuracy(accuracy);
    out = std::format("uniqCombined64State(18)({})", expr);

    return true;
}

bool HllIf::convertImpl([[maybe_unused]] String & out, [[maybe_unused]] IParser::Pos & pos)
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "{} is not yet implemented", getName());
}

bool HllMerge::convertImpl(String & out, IParser::Pos & pos)
{
    const auto fn_name = getKQLFunctionName(pos);
    if (fn_name.empty())
        return false;

    const auto arguments = getArguments(fn_name, pos, ArgumentState::Parsed, {2, 64});
    const auto arguments_as_string = std::accumulate(
        arguments.cbegin(),
        arguments.cend(),
        std::string(),
        [](const auto & acc, const auto & argument) { return acc + (acc.empty() ? "" : ", ") + argument; });

    out = std::format("uniqCombined64MergeState(18)(arrayJoin([{}]))", arguments_as_string);

    return true;
}

bool MakeBag::convertImpl([[maybe_unused]] String & out, [[maybe_unused]] IParser::Pos & pos)
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "{} is not yet implemented", getName());
}

bool MakeBagIf::convertImpl([[maybe_unused]] String & out, [[maybe_unused]] IParser::Pos & pos)
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "{} is not yet implemented", getName());
}

bool MakeList::convertImpl(String & out, IParser::Pos & pos)
{
    String fn_name = getKQLFunctionName(pos);

    if (fn_name.empty())
        return false;
    ++pos;
    const auto expr = getConvertedArgument(fn_name, pos);
    if (pos->type == TokenType::Comma)
    {
        ++pos;
        const auto max_size = getConvertedArgument(fn_name, pos);
        out = "groupArrayIf(" + max_size + ")(" + expr + " , " + expr + " IS NOT NULL)";
    }
    else
        out = "groupArrayIf(" + expr + " , " + expr + " IS NOT NULL)";
    return true;
}

bool MakeListIf::convertImpl(String & out, IParser::Pos & pos)
{
    String fn_name = getKQLFunctionName(pos);

    if (fn_name.empty())
        return false;
    ++pos;
    const auto expr = getConvertedArgument(fn_name, pos);
    ++pos;
    const auto predicate = getConvertedArgument(fn_name, pos);
    if (pos->type == TokenType::Comma)
    {
        ++pos;
        const auto max_size = getConvertedArgument(fn_name, pos);
        out = "groupArrayIf(" + max_size + ")(" + expr + " , " + predicate + " )";
    }
    else
        out = "groupArrayIf(" + expr + " , " + predicate + " )";
    return true;
}

bool MakeListWithNulls::convertImpl(String & out, IParser::Pos & pos)
{
    String fn_name = getKQLFunctionName(pos);

    if (fn_name.empty())
        return false;
    ++pos;
    const auto column_name = getConvertedArgument(fn_name, pos);
    out = "arrayConcat(groupArray(" + column_name + "), arrayMap(x -> null, range(0, toUInt32(count(*)-length(  groupArray(" + column_name
        + "))),1)))";
    return true;
}

bool MakeSet::convertImpl(String & out, IParser::Pos & pos)
{
    String fn_name = getKQLFunctionName(pos);

    if (fn_name.empty())
        return false;
    ++pos;
    const auto expr = getConvertedArgument(fn_name, pos);
    if (pos->type == TokenType::Comma)
    {
        ++pos;
        const auto max_size = getConvertedArgument(fn_name, pos);
        out = "groupUniqArray(" + max_size + ")(" + expr + ")";
    }
    else
        out = "groupUniqArray(" + expr + ")";
    return true;
}

bool MakeSetIf::convertImpl(String & out, IParser::Pos & pos)
{
    String fn_name = getKQLFunctionName(pos);

    if (fn_name.empty())
        return false;
    ++pos;
    const auto expr = getConvertedArgument(fn_name, pos);
    ++pos;
    const auto predicate = getConvertedArgument(fn_name, pos);
    if (pos->type == TokenType::Comma)
    {
        ++pos;
        const auto max_size = getConvertedArgument(fn_name, pos);
        out = "groupUniqArrayIf(" + max_size + ")(" + expr + " , " + predicate + " )";
    }
    else
        out = "groupUniqArrayIf(" + expr + " , " + predicate + " )";
    return true;
}

bool Max::convertImpl(String & out, IParser::Pos & pos)
{
    return directMapping(out, pos, "max");
}

bool MaxIf::convertImpl(String & out, IParser::Pos & pos)
{
    return directMapping(out, pos, "maxIf");
}

bool Min::convertImpl(String & out, IParser::Pos & pos)
{
    return directMapping(out, pos, "min");
}

bool MinIf::convertImpl(String & out, IParser::Pos & pos)
{
    return directMapping(out, pos, "minIf");
}

bool Percentile::convertImpl(String & out, IParser::Pos & pos)
{
    String fn_name = getKQLFunctionName(pos);

    if (fn_name.empty())
        return false;

    ++pos;
    String column_name = getConvertedArgument(fn_name, pos);
    trim(column_name);

    ++pos;
    String value = getConvertedArgument(fn_name, pos);
    trim(value);

    out = "quantile(" + value + "/100)(" + column_name + ")";
    return true;
}

bool Percentilew::convertImpl(String & out, IParser::Pos & pos)
{
    String fn_name = getKQLFunctionName(pos);

    if (fn_name.empty())
        return false;

    ++pos;
    String bucket_column = getConvertedArgument(fn_name, pos);
    trim(bucket_column);

    ++pos;
    String frequency_column = getConvertedArgument(fn_name, pos);
    trim(frequency_column);

    ++pos;
    String value = getConvertedArgument(fn_name, pos);
    trim(value);

    out = "quantileExactWeighted(" + value + "/100)(" + bucket_column + "," + frequency_column + ")";
    return true;
}

bool Percentiles::convertImpl(String & out, IParser::Pos & pos)
{
    String fn_name = getKQLFunctionName(pos);

    if (fn_name.empty())
        return false;

    ++pos;
    String column_name = getConvertedArgument(fn_name, pos);
    trim(column_name);
    String expr = "quantiles(";
    String value;
    while (pos->type != TokenType::ClosingRoundBracket)
    {
        if (pos->type != TokenType::Comma)
        {
            value = String(pos->begin, pos->end);
            expr = expr + value + "/100";
            ++pos;
            if (pos->type != TokenType::ClosingRoundBracket)
                expr += ", ";
        }
        else
            ++pos;
    }
    out = expr + ")(" + column_name + ")";
    return true;
}

bool PercentilesArray::convertImpl(String & out, IParser::Pos & pos)
{
    String fn_name = getKQLFunctionName(pos);

    if (fn_name.empty())
        return false;

    ++pos;
    String column_name = getConvertedArgument(fn_name, pos);
    trim(column_name);
    String expr = "quantiles(";
    String value;
    while (pos->type != TokenType::ClosingRoundBracket)
    {
        if (pos->type != TokenType::Comma && String(pos->begin, pos->end) != "dynamic" && pos->type != TokenType::OpeningRoundBracket
            && pos->type != TokenType::OpeningSquareBracket && pos->type != TokenType::ClosingSquareBracket)
        {
            value = String(pos->begin, pos->end);
            expr = expr + value + "/100";

            if (pos->type != TokenType::Comma && pos->type != TokenType::OpeningRoundBracket && pos->type != TokenType::OpeningSquareBracket
                && pos->type != TokenType::ClosingSquareBracket)
                expr += ", ";
            ++pos;
        }
        else
        {
            ++pos;
        }
    }
    ++pos;
    if (pos->type != TokenType::ClosingRoundBracket)
        --pos;

    expr.pop_back();
    expr.pop_back();
    expr = expr + ")(" + column_name + ")";
    out = expr;
    return true;
}

bool Percentilesw::convertImpl(String & out, IParser::Pos & pos)
{
    String fn_name = getKQLFunctionName(pos);

    if (fn_name.empty())
        return false;

    ++pos;
    String bucket_column = getConvertedArgument(fn_name, pos);
    trim(bucket_column);

    ++pos;
    String frequency_column = getConvertedArgument(fn_name, pos);
    trim(frequency_column);

    String expr = "quantilesExactWeighted(";
    String value;

    while (pos->type != TokenType::ClosingRoundBracket)
    {
        if (pos->type != TokenType::Comma)
        {
            value = String(pos->begin, pos->end);
            expr = expr + value + "/100";
            ++pos;
            if (pos->type != TokenType::ClosingRoundBracket)
                expr += ", ";
        }
        else
            ++pos;
    }
    expr = expr + ")(" + bucket_column + "," + frequency_column + ")";
    out = expr;
    return true;
}

bool PercentileswArray::convertImpl(String & out, IParser::Pos & pos)
{
    String fn_name = getKQLFunctionName(pos);

    if (fn_name.empty())
        return false;

    ++pos;
    String bucket_column = getConvertedArgument(fn_name, pos);
    trim(bucket_column);

    ++pos;
    String frequency_column = getConvertedArgument(fn_name, pos);
    trim(frequency_column);

    String expr = "quantilesExactWeighted(";
    String value;
    while (pos->type != TokenType::ClosingRoundBracket)
    {
        if (pos->type != TokenType::Comma && String(pos->begin, pos->end) != "dynamic" && pos->type != TokenType::OpeningRoundBracket
            && pos->type != TokenType::OpeningSquareBracket && pos->type != TokenType::ClosingSquareBracket)
        {
            value = String(pos->begin, pos->end);
            expr = expr + value + "/100";

            if (pos->type != TokenType::Comma && pos->type != TokenType::OpeningRoundBracket && pos->type != TokenType::OpeningSquareBracket
                && pos->type != TokenType::ClosingSquareBracket)
                expr += ", ";
            ++pos;
        }
        else
        {
            ++pos;
        }
    }
    ++pos;
    if (pos->type != TokenType::ClosingRoundBracket)
        --pos;

    expr.pop_back();
    expr.pop_back();
    expr = expr + ")(" + bucket_column + "," + frequency_column + ")";
    out = expr;
    return true;
}

bool Stdev::convertImpl(String & out, IParser::Pos & pos)
{
    String fn_name = getKQLFunctionName(pos);

    if (fn_name.empty())
        return false;
    ++pos;
    const auto expr = getConvertedArgument(fn_name, pos);
    out = "sqrt(varSamp(" + expr + "))";
    return true;
}

bool StdevIf::convertImpl(String & out, IParser::Pos & pos)
{
    String fn_name = getKQLFunctionName(pos);

    if (fn_name.empty())
        return false;
    ++pos;
    const auto expr = getConvertedArgument(fn_name, pos);
    if (pos->type != TokenType::Comma)
        return false;

    ++pos;
    const auto predicate = getConvertedArgument(fn_name, pos);
    out = "sqrt(varSampIf(" + expr + ", " + predicate + "))";
    return true;
}

bool Sum::convertImpl(String & out, IParser::Pos & pos)
{
    return directMapping(out, pos, "sum");
}

bool SumIf::convertImpl(String & out, IParser::Pos & pos)
{
    return directMapping(out, pos, "sumIf");
}

bool TakeAny::convertImpl(String & out, IParser::Pos & pos)
{
    String fn_name = getKQLFunctionName(pos);

    if (fn_name.empty())
        return false;

    String expr;
    String arg;
    const auto begin = pos;
    while (pos->type != TokenType::ClosingRoundBracket)
    {
        if (pos != begin)
            expr.append(", ");
        ++pos;
        arg = getConvertedArgument(fn_name, pos);
        expr = expr + "any(" + arg + ")";
    }
    out = expr;
    return true;
}

bool TakeAnyIf::convertImpl(String & out, IParser::Pos & pos)
{
    String fn_name = getKQLFunctionName(pos);

    if (fn_name.empty())
        return false;
    ++pos;
    const auto expr = getConvertedArgument(fn_name, pos);
    if (pos->type != TokenType::Comma)
        return false;

    ++pos;
    const auto predicate = getConvertedArgument(fn_name, pos);
    out = "anyIf(" + expr + ", " + predicate + ")";
    return true;
}

bool Variance::convertImpl(String & out, IParser::Pos & pos)
{
    return directMapping(out, pos, "varSamp");
}

bool VarianceIf::convertImpl(String & out, IParser::Pos & pos)
{
    const String fn_name = getKQLFunctionName(pos);
    if (fn_name.empty())
        return false;

    const String expr = getArgument(fn_name, pos);
    const String predicate = getArgument(fn_name, pos);
    out = std::format("varSampIf({}, {})", expr, predicate);

    return true;
}

bool VarianceP::convertImpl(String & out, IParser::Pos & pos)
{
    return directMapping(out, pos, "varPop");
}

bool CountDistinct::convertImpl(String & out, IParser::Pos & pos)
{
    const String fn_name = getKQLFunctionName(pos);
    if (fn_name.empty())
        return false;

    const String expr = getArgument(fn_name, pos);
    out = std::format("count(DISTINCT {})", expr);

    return true;
}


bool CountDistinctIf::convertImpl(String & out, IParser::Pos & pos)
{
    const String fn_name = getKQLFunctionName(pos);
    if (fn_name.empty())
        return false;

    const String expr = getArgument(fn_name, pos);
    const String predicate = getArgument(fn_name, pos);
    out = std::format("countIf(DISTINCT {}, {})", expr, predicate);

    return true;
}

}
