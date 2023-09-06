#include "ParserKQLTimespan.h"

#include <Parsers/ASTLiteral.h>
#include <Parsers/ExpressionListParsers.h>
#include <Parsers/IParserBase.h>
#include <Parsers/Kusto/ParserKQLMakeSeries.h>
#include <Parsers/Kusto/ParserKQLOperators.h>
#include <Parsers/Kusto/ParserKQLQuery.h>
#include <Parsers/ParserSelectQuery.h>
#include <Parsers/ParserTablesInSelectQuery.h>

#include <format>

namespace DB
{
namespace ErrorCodes
{
    extern const int SYNTAX_ERROR;
}

void ParserKQLMakeSeries ::parseSingleAggregationColumn(AggregationColumns & aggregation_columns, Pos begin, Pos end, size_t & column_index)
{
    std::unordered_set<String> allowed_aggregation(
        {"avg",
         "avgif",
         "count",
         "countif",
         "dcount",
         "dcountif",
         "max",
         "maxif",
         "min",
         "minif",
         "percentile",
         "take_any",
         "stdev",
         "sum",
         "sumif",
         "variance"});

    String alias;
    String aggregation_fun;
    String column;
    String default_value = "0";
    auto pos = begin;

    bool has_default = false;
    if (begin == end)
        throw Exception(ErrorCodes::SYNTAX_ERROR, "No aggregation in make-series operator");

    if (String(pos->begin, pos->end) == "=")
        throw Exception(ErrorCodes::SYNTAX_ERROR, "Invalid equal symbol (=) in make-series operator");

    auto agg_start_pos = pos;
    auto agg_end_pos = pos;
    BracketCount bracket_count;
    while (pos < end)
    {
        bracket_count.count(pos);
        if (String(pos->begin, pos->end) == "=" && bracket_count.isZero())
        {
            if (!alias.empty())
                throw Exception(ErrorCodes::SYNTAX_ERROR, "Invalid equal symbol (=) in make-series operator");
            --pos;
            if (begin != pos)
                throw Exception(ErrorCodes::SYNTAX_ERROR, "{} is not a valid alias", std::string_view(begin->begin, pos->end));
            ++pos;
            alias = String(begin->begin, begin->end);
            begin = pos;
            ++begin;
        }

        if (String(pos->begin, pos->end) == "default" && bracket_count.isZero())
        {
            has_default = true;
            if (!aggregation_fun.empty())
                throw Exception(ErrorCodes::SYNTAX_ERROR, "Extra keyword (default) in make-series operator");
            --pos;
            if (pos < begin)
                throw Exception(ErrorCodes::SYNTAX_ERROR, "No aggregation in make-series operator");

            aggregation_fun = String(begin->begin, pos->end);
            agg_start_pos = begin;
            agg_end_pos = pos;
            ++pos;
            ++pos;
            if (String(pos->begin, pos->end) != "=")
                throw Exception(ErrorCodes::SYNTAX_ERROR, "Invalid default in make-series operator");
            begin = pos;
            ++begin;
        }
        ++pos;
    }
    --end;
    if (aggregation_fun.empty())
    {
        if (end < begin)
            throw Exception(ErrorCodes::SYNTAX_ERROR, "No aggregation in make-series operator");
        aggregation_fun = String(begin->begin, end->end);
        agg_start_pos = begin;
        agg_end_pos = end;
    }
    else if (has_default)
    {
        if (end < begin)
            throw Exception(ErrorCodes::SYNTAX_ERROR, "No aggregation in make-series operator");
        default_value = getExprFromToken(String(begin->begin, end->end), pos.max_depth);
    }

    auto converted_aggregation_fun = getExprFromToken(aggregation_fun, pos.max_depth);

    auto agg_fun = String(agg_start_pos->begin, agg_start_pos->end);
    String tmp_alias;
    if (allowed_aggregation.contains(agg_fun))
        tmp_alias = agg_fun + "_";
    else
    {
        tmp_alias = std::format("Column{}", column_index);
        ++column_index;
        agg_fun.clear();
    }

    auto last_bareword_pos = agg_start_pos;
    while (agg_start_pos < agg_end_pos)
    {
        if (agg_start_pos->type == TokenType::BareWord)
            last_bareword_pos = agg_start_pos;

        if (agg_start_pos->type == TokenType::ClosingRoundBracket)
        {
            column = String(last_bareword_pos->begin, last_bareword_pos->end);
            --agg_start_pos;
            if (agg_start_pos == last_bareword_pos)
            {
                --agg_start_pos;
                if (agg_start_pos->type == TokenType::OpeningRoundBracket && !agg_fun.empty())
                    tmp_alias += column;
            }
            break;
        }
        ++agg_start_pos;
    }

    if (alias.empty())
        alias = tmp_alias;
    aggregation_columns.emplace_back(alias, converted_aggregation_fun, column, default_value);
}

bool ParserKQLMakeSeries ::parseAggregationColumns(AggregationColumns & aggregation_columns, Pos & pos)
{
    BracketCount bracket_count;
    auto begin = pos;
    size_t column_index = 1;
    while (!pos->isEnd())
    {
        bracket_count.count(pos);
        if ((pos->type == TokenType::PipeMark || pos->type == TokenType::Semicolon) && bracket_count.isZero())
            break;

        if (pos->type == TokenType::Comma && bracket_count.isZero())
        {
            parseSingleAggregationColumn(aggregation_columns, begin, pos, column_index);
            begin = pos;
            ++begin;
        }
        if (String(pos->begin, pos->end) == "on" && bracket_count.isZero())
        {
            parseSingleAggregationColumn(aggregation_columns, begin, pos, column_index);
            return true;
        }
        ++pos;
    }

    return false;
}

bool ParserKQLMakeSeries ::parseAxisColumn(String & axis_column, Pos & pos)
{
    auto begin = pos;
    BracketCount bracket_count;
    while (!pos->isEnd())
    {
        bracket_count.count(pos);
        if ((pos->type == TokenType::PipeMark || pos->type == TokenType::Semicolon) && bracket_count.isZero())
            break;

        if (auto keyword = String(pos->begin, pos->end);
            (keyword == "from" || keyword == "to" || keyword == "step") && bracket_count.isZero())
        {
            --pos;
            axis_column = String(begin->begin, pos->end);
            ++pos;
            return true;
        }

        ++pos;
    }
    return false;
}

bool ParserKQLMakeSeries ::parseFromToStepClause(FromToStepClause & from_to_step, Pos & pos)
{
    auto begin = pos;
    auto from_pos = begin;
    auto to_pos = begin;
    auto step_pos = begin;
    auto end_pos = begin;

    BracketCount bracket_count;
    while (!pos->isEnd())
    {
        bracket_count.count(pos);
        if ((pos->type == TokenType::PipeMark || pos->type == TokenType::Semicolon) && bracket_count.isZero())
            break;

        if (String(pos->begin, pos->end) == "from" && bracket_count.isZero())
            from_pos = pos;
        if (String(pos->begin, pos->end) == "to" && bracket_count.isZero())
            to_pos = pos;
        if (String(pos->begin, pos->end) == "step" && bracket_count.isZero())
            step_pos = pos;
        if (String(pos->begin, pos->end) == "by" && bracket_count.isZero())
        {
            end_pos = pos;
            break;
        }
        ++pos;
    }

    if (end_pos == begin)
        end_pos = pos;

    if (String(step_pos->begin, step_pos->end) != "step")
        return false;

    if (String(from_pos->begin, from_pos->end) == "from")
    {
        ++from_pos;
        auto end_from_pos = (to_pos != begin) ? to_pos : step_pos;
        --end_from_pos;
        from_to_step.from_str = String(from_pos->begin, end_from_pos->end);
    }

    if (String(to_pos->begin, to_pos->end) == "to")
    {
        ++to_pos;
        --step_pos;
        from_to_step.to_str = String(to_pos->begin, step_pos->end);
        ++step_pos;
    }
    --end_pos;
    ++step_pos;
    from_to_step.step_str = String(step_pos->begin, end_pos->end);

    if (std::optional<Int64> ticks; String(step_pos->begin, step_pos->end) == "time" || String(step_pos->begin, step_pos->end) == "timespan"
        || ParserKQLTimespan::tryParse(from_to_step.step_str, ticks))
    {
        // TODO: this is a hack of the ugliest kind that can only be fixed by supporting arbitrary expressions in make-series
        static constexpr std::string_view wrapper = "toIntervalNanosecond(";
        const auto timespan = getExprFromToken(from_to_step.step_str, pos.max_depth);
        const auto value = timespan.substr(wrapper.length(), timespan.length() - wrapper.length() - 1);

        from_to_step.is_timespan = true;
        from_to_step.step = std::stod(value) * 1e-9;
    }
    else
        from_to_step.step = std::stod(from_to_step.step_str);

    return true;
}

bool ParserKQLMakeSeries ::makeSeries(KQLMakeSeries & kql_make_series, ASTPtr & select_node, const uint32_t & max_depth)
{
    const uint64_t era_diff
        = 62135596800; // this magic number is the differicen is second form 0001-01-01 (Azure start time ) and 1970-01-01 (CH start time)

    String start_str, end_str;
    String sub_query, main_query;

    const auto & aggregation_columns = kql_make_series.aggregation_columns;
    const auto & from_to_step = kql_make_series.from_to_step;
    auto & subquery_columns = kql_make_series.subquery_columns;
    auto & axis_column = kql_make_series.axis_column;
    auto & group_expression = kql_make_series.group_expression;
    auto step = from_to_step.step;

    if (!kql_make_series.from_to_step.from_str.empty())
        start_str = getExprFromToken(kql_make_series.from_to_step.from_str, max_depth);

    if (!kql_make_series.from_to_step.to_str.empty())
        end_str = getExprFromToken(from_to_step.to_str, max_depth);

    auto date_type_cast = [&](const String & src)
    {
        Tokens tokens(src.c_str(), src.c_str() + src.size());
        IParser::Pos pos(tokens, max_depth);
        String res;
        while (!pos->isEnd())
        {
            auto tmp = String(pos->begin, pos->end);
            if (tmp == "kql_datetime" || tmp == "kql_todatetime")
            {
                ++pos;
                auto datetime_start_pos = pos;
                auto datetime_end_pos = pos;
                BracketCount bracket_count;
                while (!pos->isEnd())
                {
                    bracket_count.count(pos);
                    if (pos->type == TokenType::ClosingRoundBracket && bracket_count.isZero())
                    {
                        ++datetime_start_pos;
                        datetime_end_pos = pos;
                        --datetime_end_pos;
                        tmp = std::format("toDateTime64({}, 9, 'UTC')", String(datetime_start_pos->begin, datetime_end_pos->end));
                        break;
                    }
                    ++pos;
                }
            }
            res = res.empty() ? tmp : res + " " + tmp;
            ++pos;
        }
        return res;
    };

    start_str = date_type_cast(start_str);
    end_str = date_type_cast(end_str);

    String bin_str, start, end;

    uint64_t diff = 0;
    String axis_column_format;
    String axis_str;

    auto get_group_expression_alias = [&]
    {
        std::vector<String> group_expression_tokens;
        Tokens tokens(group_expression.c_str(), group_expression.c_str() + group_expression.size());
        IParser::Pos pos(tokens, max_depth);
        while (!pos->isEnd())
        {
            if (String(pos->begin, pos->end) == "AS")
            {
                if (!group_expression_tokens.empty())
                    group_expression_tokens.pop_back();
                ++pos;
                group_expression_tokens.emplace_back(pos->begin, pos->end);
            }
            else
                group_expression_tokens.emplace_back(pos->begin, pos->end);
            ++pos;
        }
        String res;
        for (auto const & token : group_expression_tokens)
            res = res + token + " ";
        return res;
    };

    auto group_expression_alias = get_group_expression_alias();

    if (from_to_step.is_timespan)
    {
        axis_column_format = std::format("toFloat64(toDateTime64({}, 9, 'UTC'))", axis_column);
        if (!start_str.empty())
        {
            start_str = std::format("date_trunc('second', {})", start_str);
        }
    }
    else
        axis_column_format = std::format("toFloat64({})", axis_column);

    if (!start_str.empty()) // has from
    {
        bin_str = std::format(
            " toFloat64({0}) + (toInt64((({1} - toFloat64({0})) / {2})) * {2}) AS {3}_ali",
            start_str,
            axis_column_format,
            step,
            axis_column);
        start = std::format("toUInt64({})", start_str);
    }
    else
    {
        if (from_to_step.is_timespan)
            diff = era_diff;
        bin_str = std::format(" toFloat64(toInt64(({0} + {1}) / {2}) * {2}) AS {3}_ali ", axis_column_format, diff, step, axis_column);
    }

    if (!end_str.empty())
        end = std::format("toUInt64({})", end_str);

    String range;
    String condition;

    if (!start_str.empty() && !end_str.empty())
    {
        range = std::format("range({}, {}, toUInt64({}))", start, end, step);
        condition = std::format("where toInt64({0}) >= {1} and toInt64({0}) < {2}", axis_column_format, start, end);
    }
    else if (start_str.empty() && !end_str.empty())
    {
        range = std::format("range(low, {} + {}, toUInt64({}))", end, diff, step);
        condition = std::format("where toInt64({0}) - {1} < {2}", axis_column_format, diff, end);
    }
    else if (!start_str.empty() && end_str.empty())
    {
        range = std::format("range({}, high, toUInt64({}))", start, step);
        condition = std::format("where toInt64({}) >= {}", axis_column_format, start);
    }
    else
    {
        range = std::format("range(low, high, toUInt64({}))", step);
        condition = " ";
    }

    auto range_len = std::format("length({})", range);

    String sub_sub_query;
    if (group_expression.empty())
        sub_sub_query = std::format(
            " (Select {0}, {1} FROM {2} {4} GROUP BY {3}_ali ORDER BY {3}_ali) ",
            subquery_columns,
            bin_str,
            "table_name",
            axis_column,
            condition);
    else
        sub_sub_query = std::format(
            " (Select {0}, {1}, {2} FROM {3} {5} GROUP BY {0}, {4}_ali ORDER BY {4}_ali) ",
            group_expression,
            subquery_columns,
            bin_str,
            "table_name",
            axis_column,
            condition);

    ASTPtr sub_query_node;

    if (!ParserSimpleCHSubquery(select_node).parseByString(sub_sub_query, sub_query_node, max_depth))
        return false;
    select_node->as<ASTSelectQuery>()->setExpression(ASTSelectQuery::Expression::TABLES, std::move(sub_query_node));

    if (!group_expression.empty())
        main_query = std::format("{} ", group_expression_alias);

    auto axis_and_agg_alias_list = axis_column;
    auto final_axis_agg_alias_list = std::format("tupleElement(zipped,1) AS {}", axis_column);
    int idx = 2;
    int agg_count = 1;
    for (auto agg_column : aggregation_columns)
    {
        String agg_group_column = std::format(
            "arrayConcat(groupArray({0}_ali) as ga{3}, arrayMap(x -> ({1}),range(0,toUInt32({2} - length(ga{3}) < 0 ? 0 : {2} - length(ga{3})),1))) as "
            "{0}",
            agg_column.alias,
            agg_column.default_value,
            range_len,
            agg_count
            );
        main_query = main_query.empty() ? agg_group_column : main_query + ", " + agg_group_column;

        axis_and_agg_alias_list += ", " + agg_column.alias;
        final_axis_agg_alias_list += std::format(", tupleElement(zipped,{}) AS {}", idx, agg_column.alias);
        ++agg_count;
    }

    if (from_to_step.is_timespan)
        axis_str = std::format(
            "arrayDistinct(arrayConcat(groupArray(toDateTime64({0}_ali - {1}, 9, 'UTC')), arrayMap(x->(toDateTime64(x - {1}, 9, 'UTC')), "
            "{2}))) as {0}",
            axis_column,
            diff,
            range);
    else
        axis_str
            = std::format("arrayDistinct(arrayConcat(groupArray({0}_ali), arrayMap(x->(toFloat64(x)), {1}))) as {0}", axis_column, range);

    main_query += ", " + axis_str;
    auto sub_group_by = group_expression.empty() ? "" : std::format("GROUP BY {}", group_expression_alias);

    sub_query = std::format(
        "( SELECT toUInt64(min({}_ali)) AS low, toUInt64(max({}_ali))+ {} AS high, arraySort(arrayZip({})) as zipped, {} FROM {} {} )",
        axis_column,
        axis_column,
        step,
        axis_and_agg_alias_list,
        main_query,
        sub_sub_query,
        sub_group_by);

    if (group_expression.empty())
        main_query = std::format("{}", final_axis_agg_alias_list);
    else
        main_query = std::format("{},{}", group_expression_alias, final_axis_agg_alias_list);

    if (!ParserSimpleCHSubquery(select_node).parseByString(sub_query, sub_query_node, max_depth))
        return false;
    select_node->as<ASTSelectQuery>()->setExpression(ASTSelectQuery::Expression::TABLES, std::move(sub_query_node));

    kql_make_series.sub_query = std::move(sub_query);
    kql_make_series.main_query = std::move(main_query);

    return true;
}

bool ParserKQLMakeSeries ::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    auto begin = pos;
    ParserKeyword s_on("on");
    ParserKeyword s_by("by");

    ASTPtr select_expression_list;

    KQLMakeSeries kql_make_series;
    auto & aggregation_columns = kql_make_series.aggregation_columns;
    auto & from_to_step = kql_make_series.from_to_step;
    auto & subquery_columns = kql_make_series.subquery_columns;
    auto & axis_column = kql_make_series.axis_column;
    auto & group_expression = kql_make_series.group_expression;

    if (!parseAggregationColumns(aggregation_columns, pos))
        return false;

    if (!s_on.ignore(pos, expected))
        return false;

    if (!parseAxisColumn(axis_column, pos))
        return false;

    if (!parseFromToStepClause(from_to_step, pos))
        return false;

    if (s_by.ignore(pos, expected))
    {
        group_expression = getExprFromToken(pos);
        if (group_expression.empty())
            return false;
    }

    for (auto & agg_column : aggregation_columns)
    {
        String column_str = std::format("{} AS {}_ali", agg_column.aggregation_fun, agg_column.alias);
        subquery_columns = subquery_columns.empty() ? column_str : subquery_columns + ", " + column_str;
    }

    makeSeries(kql_make_series, node, pos.max_depth);

    Tokens token_main_query(kql_make_series.main_query.c_str(), kql_make_series.main_query.c_str() + kql_make_series.main_query.size());
    IParser::Pos pos_main_query(token_main_query, pos.max_depth);

    if (!ParserNotEmptyExpressionList(true).parse(pos_main_query, select_expression_list, expected))
        return false;
    node->as<ASTSelectQuery>()->setExpression(ASTSelectQuery::Expression::SELECT, std::move(select_expression_list));

    pos = begin;
    return true;
}

}
