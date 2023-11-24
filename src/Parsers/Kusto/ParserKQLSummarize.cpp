#include <IO/ReadBufferFromString.h>
#include <IO/ReadHelpers.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTInterpolateElement.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTOrderByElement.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/ExpressionListParsers.h>
#include <Parsers/IParserBase.h>
#include <Parsers/ASTSubquery.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/Kusto/ParserKQLQuery.h>
#include <Parsers/Kusto/ParserKQLSummarize.h>
#include <Parsers/Kusto/KustoFunctions/IParserKQLFunction.h>
#include <Parsers/ParserSelectQuery.h>
#include <Parsers/ParserSetQuery.h>
#include <Parsers/ParserTablesInSelectQuery.h>
#include <Parsers/ParserWithElement.h>
#include "Utilities.h"

#include <format>
#include <memory>
#include <queue>
#include <vector>
#include <sstream>
#include <string>

namespace DB
{

namespace ErrorCodes
{
    extern const int SYNTAX_ERROR;
}


bool ParserKQLSummarize::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ASTPtr select_expression_list;
    ASTPtr group_expression_list;

    String expr_aggregation;
    String expr_groupby;
    String expr_columns;
    bool groupby = false;
    auto column_begin_pos = pos;

    uint16_t bracket_count = 0;
    int32_t new_column_index = 1;

    bool require_subquery = false;
    std::vector<String> expr_aggregations;
    std::vector<String> expr_groupbys;
    std::vector<String> groupby_identifiers;
    std::unordered_map <String,String> alias_column_map;
    std::unordered_map <String,String> column_alias_map;

    std::unordered_set<String> aggregate_functions(
        {"arg_max",
         "arg_min",
         "avg",
         "avgif",
         "binary_all_and",
         "binary_all_or",
         "binary_all_xor",
         "buildschema",
         "count",
         "countif",
         "dcount",
         "dcountif",
         "make_bag",
         "make_bag_if",
         "make_list",
         "make_list_if",
         "make_list_with_nulls",
         "make_set",
         "make_set_if",
         "max",
         "maxif",
         "min",
         "minif",
         "percentile",
         "percentilew",
         "percentiles",
         "percentiles_array",
         "percentilesw",
         "percentilesw_array",
         "stdev",
         "stdevif",
         "sum",
         "sumif",
         "take_any",
         "take_anyif",
         "variance",
         "varianceif",
         "variancep"});

    auto apply_aliais = [&](Pos & begin_pos, Pos & end_pos, bool is_groupby)
    {
        if (String(begin_pos->begin, begin_pos->end) == "by")
            return;
        if (end_pos->end <= begin_pos->begin)
            throw Exception(ErrorCodes::SYNTAX_ERROR, "Syntax error near keyword \"{}\"", std::string_view(begin_pos->begin, begin_pos->end));
        auto expr = String(begin_pos->begin, end_pos->end);
        auto equal_pos = begin_pos;
        ++equal_pos;
        String alias;
        if (!is_groupby)
        {
            if (String(equal_pos->begin, equal_pos->end) != "=")
            {
                String aggregate_fun = String(begin_pos->begin, begin_pos->end);
                if (aggregate_functions.find(aggregate_fun) == aggregate_functions.end())
                {
                    alias = std::format("Columns{}", new_column_index);
                    ++new_column_index;
                }
                else
                {
                    alias = std::format("{}_", aggregate_fun);
                    auto agg_colum_pos = begin_pos;
                    ++agg_colum_pos;
                    ++agg_colum_pos;
                    ++agg_colum_pos;
                    if (agg_colum_pos->type == TokenType::Comma || agg_colum_pos->type == TokenType::ClosingRoundBracket)
                    {
                        --agg_colum_pos;
                        if (agg_colum_pos->type != TokenType::ClosingRoundBracket)
                            alias = alias + String(agg_colum_pos->begin, agg_colum_pos->end);
                    }
                }
                
            }
            else
            {
                alias = String(begin_pos->begin, begin_pos->end);
                begin_pos = equal_pos;
                ++begin_pos;
            }

            if (!require_subquery)
            {
                String replacedExpr;
                while (begin_pos <= end_pos)
                {
                    auto token = String(begin_pos->begin, begin_pos->end);
                    if (alias_column_map.contains(token))
                        replacedExpr = replacedExpr + " " + alias_column_map[token];
                    else
                        replacedExpr = replacedExpr + " " + token;
                    ++begin_pos;
                }
                
                expr = alias.empty() ? expr : std::format("{} = {}", alias, replacedExpr);
            }
            expr_aggregations.push_back(expr);
        }
        else
        {
            if (String(equal_pos->begin, equal_pos->end) != "=")
            {
                String groupby_fun = String(begin_pos->begin, begin_pos->end);
                if (equal_pos->isEnd() || equal_pos->type == TokenType::Comma || equal_pos->type == TokenType::Semicolon
                    || equal_pos->type == TokenType::PipeMark)
                {
                    expr = groupby_fun;
                }
                else
                {
                    
                    if (groupby_fun == "bin" || groupby_fun == "bin_at")
                    {
                        auto bin_colum_pos = begin_pos;
                        ++bin_colum_pos;
                        ++bin_colum_pos;
                        alias = String(bin_colum_pos->begin, bin_colum_pos->end);
                        ++bin_colum_pos;
                        if (bin_colum_pos->type != TokenType::Comma)
                            alias.clear();
                    }
                    if (alias.empty())
                    {
                        alias = std::format("Columns{}", new_column_index);
                        ++new_column_index;
                    }

                }
                 expr = alias.empty() ? expr : std::format("{} = {}", alias, expr);
            }
            IParserKQLFunction::getIdentifiers(begin_pos, end_pos, groupby_identifiers);
            expr_groupbys.push_back(expr);
        }
    };

    auto select_expr = node->as<ASTSelectQuery>()->select();
    if (select_expr)
    {
        auto aliais_is_column_only = [&](String &column_name)
        {
            Tokens token_column_name(column_name.c_str(), column_name.c_str() + column_name.size());
            IParser::Pos pos_token_column_name(token_column_name, pos.max_depth);
            ++pos_token_column_name;
            return !pos_token_column_name.isValid();
        };

        std::ranges::for_each(
            node->as<ASTSelectQuery>()->select()->children,
            [&](const ASTPtr & expression)
            //[&alias_column_map, &column_alias_map, &aliais_is_column_only](const ASTPtr & expression)
            {
                if (const auto alias = expression->tryGetAlias(); !alias.empty())
                {
                    auto column_name = expression->getColumnNameWithoutAlias();
                    alias_column_map[alias] = column_name;
                    column_alias_map[column_name] = alias;
                    if (!aliais_is_column_only(column_name))
                        require_subquery = true;
                }
            });
    }

    while (!pos->isEnd() && pos->type != TokenType::PipeMark && pos->type != TokenType::Semicolon)
    {
        if (pos->type == TokenType::OpeningRoundBracket)
            ++bracket_count;

        if (pos->type == TokenType::ClosingRoundBracket)
            --bracket_count;

        if ((bracket_count == 0 and pos->type == TokenType::Comma) || String(pos->begin, pos->end) == "by")
        {
            auto end_pos = pos;
            --end_pos;
            apply_aliais(column_begin_pos, end_pos, groupby);
            if (String(pos->begin, pos->end) == "by")
                groupby = true;
            column_begin_pos = pos;
            ++column_begin_pos;
        }
        ++pos;
    }
    --pos;
    apply_aliais(column_begin_pos, pos, groupby);

    for (auto const & expr : expr_aggregations)
        expr_aggregation = expr_aggregation.empty() ? expr : expr_aggregation + "," + expr;

    for (auto const & expr : expr_groupbys)
        expr_groupby = expr_groupby.empty() ? expr : expr_groupby + "," + expr;

    if (!expr_groupby.empty())
        expr_columns = expr_groupby;

    if (require_subquery)
    {
        if (!expr_aggregation.empty())
        {
            if (expr_columns.empty())
                expr_columns = expr_aggregation;
            else
                expr_columns = expr_columns + "," + expr_aggregation;
        }
    }
    else
        expr_columns = expr_aggregation;
    String converted_columns_raw = getExprFromToken(expr_columns, pos.max_depth);
    String converted_columns;
    if (require_subquery)
    {
        converted_columns = std::move(converted_columns_raw);
    }
    else
    {
        for (auto identifier : groupby_identifiers) 
        {
        converted_columns += alias_column_map.contains(identifier) ? alias_column_map[identifier] + " as " + identifier : identifier;
        converted_columns +=",";
        }
        converted_columns += std::move(converted_columns_raw);
    }

    Tokens token_converted_columns(converted_columns.c_str(), converted_columns.c_str() + converted_columns.size());
    IParser::Pos pos_converted_columns(token_converted_columns, pos.max_depth);

    if (!ParserNotEmptyExpressionList(true).parse(pos_converted_columns, select_expression_list, expected))
        return false;

    ASTPtr select_node;
    if (require_subquery)
    {
        select_node= std::make_shared<ASTSelectQuery>();
        apply_subquery(select_node, node);
    }
    else
        select_node = std::move(node);

    select_node->as<ASTSelectQuery>()->setExpression(ASTSelectQuery::Expression::SELECT, std::move(select_expression_list));
    select_node->as<ASTSelectQuery>()->setExpression(ASTSelectQuery::Expression::ORDER_BY, nullptr);

    if (groupby)
    {
        String converted_groupby = getExprFromToken(expr_groupby, pos.max_depth);

        Tokens token_converted_groupby(converted_groupby.c_str(), converted_groupby.c_str() + converted_groupby.size());
        IParser::Pos postoken_converted_groupby(token_converted_groupby, pos.max_depth);

        if (!ParserNotEmptyExpressionList(false).parse(postoken_converted_groupby, group_expression_list, expected))
            return false;
        select_node->as<ASTSelectQuery>()->setExpression(ASTSelectQuery::Expression::GROUP_BY, std::move(group_expression_list));
    }

    node = std::move(select_node);
    return true;
}
}
