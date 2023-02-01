#include <Parsers/ASTSubquery.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/ASTWithElement.h>
#include <Parsers/ExpressionListParsers.h>
#include <Parsers/Kusto/ParserKQLQuery.h>
#include <Parsers/Kusto/ParserKQLTopNested.h>
#include <Parsers/ParserSelectQuery.h>

#include <format>

namespace DB
{

namespace ErrorCodes
{
    extern const int UNKNOWN_DIRECTION_OF_SORTING;
    extern const int SYNTAX_ERROR;
}

String ParserKQLTopNested ::calculateSingleTopNestedWithOthers(
    const TopNestedClauses & top_nested_clauses, size_t layer, bool has_others, const uint32_t max_depth)
{
    const String source_table = "source_table AS (SELECT * FROM StormEvents) ";
    const String & topn = getExprFromToken(top_nested_clauses[layer].topn, max_depth);
    const String & expr_alias = top_nested_clauses[layer].expr_alias;
    const String & expr = getExprFromToken(top_nested_clauses[layer].expr, max_depth);
    const String & agg_alias = top_nested_clauses[layer].agg_alias;
    const String & agg_expr = getExprFromToken(top_nested_clauses[layer].agg_expr, max_depth);
    const String & order_expr = top_nested_clauses[layer].order;

    String topn_expr = topn.empty() ? "" : std::format("LIMIT {} ", topn);
    String column_expr_with_aliais = expr + " AS " + expr_alias;
    String agg_expr_with_aliais = std::format("{} AS {} ", agg_expr, agg_alias);
    String agg_expr_value_with_aliais = std::format("{} AS {}_value ", agg_expr, agg_alias);
    String query;
    if (layer == 0)
    {
        query = std::format(
            "WITH {0},tb0_normal AS (SELECT {1}, {2} FROM source_table GROUP BY {3} ORDER BY {4} {5} {6})",
            source_table,
            column_expr_with_aliais,
            agg_expr_with_aliais,
            expr_alias,
            agg_alias,
            order_expr,
            topn_expr);
        if (has_others)
            query = query
                + std::format(
                        ",tb0_others AS (SELECT {0} FROM source_table WHERE {1} NOT IN (SELECT {1} FROM tb0_normal))",
                        agg_expr_value_with_aliais,
                        expr_alias);
    }
    else
    {
        const String tb0_normal_name = std::format("tb{}_normal", layer - 1);
        const String row_alias0_name = std::format("row{}", layer - 1);

        const String tb1_prev_name = std::format("tb{}_prev", layer);
        const String tb1_partition_name = std::format("tb{}_partition", layer);
        const String tb1_normal_name = std::format("tb{}_normal", layer);
        const String tb1_others_prev_name = std::format("tb{}_others_prev", layer);
        const String tb1_others_name = std::format("tb{}_others", layer);
        const String row_alias1_name = std::format("row{}", layer);

        String column_list, select_list, join_list, group_list, prev_group_list;
        for (size_t i = 0; i < layer; ++i)
        {
            const String select_tmp = std::format("{0}, {1}", top_nested_clauses[i].expr_alias, top_nested_clauses[i].agg_alias);
            select_list = select_list.empty() ? select_tmp : select_list + ", " + select_tmp;
            join_list = join_list.empty() ? top_nested_clauses[i].expr : join_list + ", " + top_nested_clauses[i].expr;
            column_list = column_list.empty() ? top_nested_clauses[i].expr_alias : column_list + ", " + top_nested_clauses[i].expr_alias;

            prev_group_list = select_list;
        }
        group_list = select_list + ", " + expr_alias;
        auto tb1_prev_select_list = select_list + ", " + column_expr_with_aliais + ", " + agg_expr_with_aliais;
        auto tb1_partition_select_list = select_list + ", " + expr_alias + ", " + agg_alias;
        auto tb1_others_select_list = select_list + ", " + expr_alias + ", " + agg_alias;


        const String tb1_prev_query = std::format(
            "{0} AS (SELECT {1} FROM {2} INNER JOIN source_table AS join1 USING ({3}) GROUP BY {4})",
            tb1_prev_name,
            tb1_prev_select_list,
            tb0_normal_name,
            join_list,
            group_list);

        const String tb1_partition_query = std::format(
            "{0} AS (SELECT {1}, ROW_NUMBER () over (PARTITION by {2} order by {3} {4}) AS {5} FROM {6})",
            tb1_partition_name,
            tb1_partition_select_list,
            column_list,
            agg_alias,
            order_expr,
            row_alias1_name,
            tb1_prev_name);

        const String where_clause = (topn.empty() || layer < 1) ? "" : std::format("WHERE {} <= {}", row_alias1_name, topn);
        const String tb1_normal_query
            = std::format("{0} AS (SELECT {1} FROM {2} {3})", tb1_normal_name, tb1_partition_select_list, tb1_partition_name, where_clause);

        query = tb1_prev_query + "," + tb1_partition_query + "," + tb1_normal_query;
        if (has_others)
        {
            auto tb1_others_prev_select_list = column_list + ", " + agg_expr_value_with_aliais;
            auto tb1_others_prev_join_clause
                = std::format("LEFT JOIN {0} USING ({1})", tb1_normal_name, column_list + ", " + expr_alias);
            auto tb1_others_prev_join_where_clasue = std::format(" empty({}.{}) ", tb1_normal_name, expr);
            for (size_t i = 0; i < layer; ++i)
                tb1_others_prev_join_where_clasue
                    += std::format("AND source_table.{0} IN (SELECT {0} FROM {1}) ", top_nested_clauses[i].expr, tb1_normal_name);

            const String tb1_others_prev_query = std::format(
                "{0} AS (SELECT {1} FROM source_table {2} WHERE {3} GROUP BY {4})",
                tb1_others_prev_name,
                tb1_others_prev_select_list,
                tb1_others_prev_join_clause,
                tb1_others_prev_join_where_clasue,
                column_list);

            const String tb1_others_query = std::format(
                "{0} AS (SELECT DISTINCT {1}, {2}_value FROM {3} RIGHT JOIN {4} USING ({5}))",
                tb1_others_name,
                select_list,
                agg_alias,
                tb1_others_prev_name,
                tb1_normal_name,
                column_list);

            query = query + "," + tb1_others_prev_query + "," + tb1_others_query;
        }
    }
    return query;
}

String ParserKQLTopNested ::calculateTopNestedWithOthers(const TopNestedClauses & top_nested_clauses, const uint32_t max_depth)
{
    String query, last_select_list, last_others_list;
    auto size = top_nested_clauses.size();
    bool has_others = false;
    for (size_t i = 0; i < size; ++i)
    {
        if (!top_nested_clauses[i].others.empty())
        {
            has_others = true;
            break;
        }
    }

    for (size_t i = 0; i < size; ++i)
    {
        const String single_query = calculateSingleTopNestedWithOthers(top_nested_clauses, i, has_others, max_depth);
        const String others_expr
            = top_nested_clauses[i].others.empty() ? "NULL" : getExprFromToken(top_nested_clauses[i].others, max_depth);
        const String others_agg = top_nested_clauses[i].others.empty() ? "NULL" : std::format("{}_value", top_nested_clauses[i].agg_alias);
        if (i == 0)
        {
            query = single_query;
            last_select_list = std::format("{}, {}", top_nested_clauses[i].expr_alias, top_nested_clauses[i].agg_alias);
            last_others_list = std::format(
                "{} AS {}, {} AS {}", others_expr, top_nested_clauses[i].expr_alias, others_agg, top_nested_clauses[i].agg_alias);
        }
        else
        {
            query = query + "," + single_query;
            last_others_list
                = last_select_list + ", "
                + std::format(
                      "{} AS {}, {} AS {}", others_expr, top_nested_clauses[i].expr_alias, others_agg, top_nested_clauses[i].agg_alias);
            last_select_list
                = last_select_list + ", " + std::format("{}, {}", top_nested_clauses[i].expr_alias, top_nested_clauses[i].agg_alias);
        }
    }
    if (has_others)
        for (size_t i = 0; i < size - 1; ++i)
        {
            auto other_values = top_nested_clauses[i].agg_alias;
            String all_others_table = std::format("tb{}_all_others AS (SELECT ", i);
            String separator;
            String first_list;
            for (size_t j = 0; j < i; ++j)
            {
                if (first_list.empty())
                    first_list = std::format("{}, {}", top_nested_clauses[j].expr_alias, top_nested_clauses[j].agg_alias);
                else
                    first_list += std::format(", {}, {}", top_nested_clauses[j].expr_alias, top_nested_clauses[j].agg_alias);
            }
            all_others_table += first_list;
            for (size_t j = i; j < size; ++j)
            {
                separator = (i == 0) ? "" : ",";
                if (i == 0)
                {
                    separator = (j == 0) ? "" : ",";
                }
                else
                    separator = ",";
                if (top_nested_clauses[j].others.empty())
                    all_others_table
                        = all_others_table
                        + std::format(
                              "{} NULL AS {} , NULL AS {}", separator, top_nested_clauses[j].expr_alias, top_nested_clauses[j].agg_alias);
                else
                    all_others_table = all_others_table
                        + std::format("{} {} AS {} , {}_value AS {}",
                                      separator,
                                      getExprFromToken(top_nested_clauses[j].others, max_depth),
                                      top_nested_clauses[j].expr_alias,
                                      other_values,
                                      top_nested_clauses[j].agg_alias);
            }
            all_others_table += std::format(" FROM tb{}_others )", i);
            query = query + "," + all_others_table;
        }

    String last_normal_table = std::format("tb{}_normal", size - 1);
    if (has_others)
    {
        String last_others_table = std::format("tb{}_others", size - 1);
        query = query
            + std::format(
                    ", last_query AS (SELECT {0} FROM {1} UNION ALL SELECT {2} FROM {3}",
                    last_select_list,
                    last_normal_table,
                    last_others_list,
                    last_others_table);
        if (size > 1)
        {
            for (size_t i = 0; i < size - 1; ++i)
            {
                String tb_all_others = std::format("tb{}_all_others", i);
                query = query + std::format(" UNION ALL SELECT {} FROM {}", last_select_list, tb_all_others);
            }
        }
        query += ") Select * from last_query";
    }
    else
        query = query + std::format(" SELECT {0} FROM {1} ", last_select_list, last_normal_table);

    return query;
}

bool ParserKQLTopNested ::parseSingleTopNestedClause(Pos & begin_pos, Pos & last_pos, TopNestedClause & top_nested_clause, const int layer)
{
    TopNestedClause arg;
    auto pos = begin_pos;
    for (auto i = 0; i < 3; ++i)
        ++pos;
    auto start_pos = pos;
    auto end_pos = pos;

    auto get_name_value = [&](Pos & begin, Pos & end, String & name, String & value)
    {
        Pos tmp = begin;
        bool has_alias = false;
        Pos value_pos = begin;
        while (tmp < end)
        {
            if (String(tmp->begin, tmp->end) == "=")
            {
                --tmp;
                name = String(begin->begin, tmp->end);
                ++tmp;
                ++tmp;
                --end;
                value = String(tmp->begin, end->end);
                value_pos = tmp;
                ++end;
                has_alias = true;
                break;
            }
            ++tmp;
        }
        if (!has_alias)
        {
            --end;
            value = String(begin->begin, end->end);
            ++end;
        }
        return value_pos;
    };

    bool has_by = false, has_of = false;
    Pos expr_start_pos = begin_pos;
    Pos expr_end_pos = begin_pos;
    while (pos < last_pos)
    {
        if (String(pos->begin, pos->end) == "of")
        {
            has_of = true;
            end_pos = pos;
            --end_pos;
            if (start_pos <= end_pos)
                arg.topn = String(start_pos->begin, end_pos->end);
            start_pos = pos;
            ++start_pos;
        }

        if (String(pos->begin, pos->end) == "with")
        {
            end_pos = pos;
            expr_start_pos = get_name_value(start_pos, end_pos, arg.expr_alias, arg.expr);
            expr_end_pos = end_pos;
            start_pos = pos;
            ++start_pos;
        }

        if (String(pos->begin, pos->end) == "by")
        {
            has_by = true;
            end_pos = pos;
            if (arg.expr.empty())
            {
                expr_start_pos = get_name_value(start_pos, end_pos, arg.expr_alias, arg.expr);
                expr_end_pos = end_pos;
            }
            else
                get_name_value(start_pos, end_pos, arg.others_name, arg.others);
            start_pos = pos;
            ++start_pos;
        }
        ++pos;
    }

    if (!has_of)
        throw Exception("Missing 'of' keyword for top-nested operator", ErrorCodes::SYNTAX_ERROR);

    if (!has_by)
        throw Exception("Missing 'by' keyword for top-nested operator", ErrorCodes::SYNTAX_ERROR);

    get_name_value(start_pos, pos, arg.agg_alias, arg.agg_expr);

    if (arg.agg_expr.empty())
        throw Exception("Missing aggregation expression for top-nested operator", ErrorCodes::SYNTAX_ERROR);

    if (arg.expr_alias.empty())
    {   --expr_end_pos;
        if (expr_start_pos == expr_end_pos)
            arg.expr_alias = arg.expr;
        else
            arg.expr_alias = std::format("Column{}", layer + 1);
    }

    if (arg.agg_alias.empty())
        arg.agg_alias = std::format("aggregated_{}", arg.expr_alias);

    --last_pos;

    if (last_pos->type != TokenType::BareWord)
    {
        if (last_pos->type != TokenType::Number && last_pos->type != TokenType::ClosingRoundBracket)
            throw Exception("Incorrect aggregation expression : " + arg.expr, ErrorCodes::SYNTAX_ERROR);
        arg.order = "DESC";
    }
    else
    {
        const auto sort_direct = String(last_pos->begin, last_pos->end);
        if (sort_direct != "desc" && sort_direct != "asc")
            throw Exception("Unknown direction of sorting : " + sort_direct, ErrorCodes::UNKNOWN_DIRECTION_OF_SORTING);

        std::size_t found = arg.agg_expr.find(sort_direct);
        arg.agg_expr = arg.agg_expr.substr(0, found);
        arg.order = sort_direct;
    }

    top_nested_clause = std::move(arg);
    return true;
}

bool ParserKQLTopNested ::parseTopNestedClause(Pos & pos, TopNestedClauses & top_nested_clauses)
{
    TopNestedClause top_nested_clause;
    auto start_pos = pos;
    for (auto i = 0; i < 3; ++i)
        --start_pos;

    auto end_pos = start_pos;
    auto paren_count = 0;
    int layer = 0;
    while (!pos->isEnd() && pos->type != TokenType::PipeMark && pos->type != TokenType::Semicolon)
    {
        if (pos->type == TokenType::ClosingRoundBracket)
            --paren_count;
        if (pos->type == TokenType::OpeningRoundBracket)
            ++paren_count;

        if (String(pos->begin, pos->end) == "," and paren_count == 0)
        {
            end_pos = pos;
            parseSingleTopNestedClause(start_pos, end_pos, top_nested_clause, layer);
            ++layer;
            top_nested_clauses.emplace_back(top_nested_clause);
            start_pos = pos;
            ++start_pos;
        }
        ++pos;
    }

    parseSingleTopNestedClause(start_pos, pos, top_nested_clause, layer);
    top_nested_clauses.emplace_back(top_nested_clause);
    return true;
}

bool ParserKQLTopNested ::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    TopNestedClauses top_nested_clauses;

    parseTopNestedClause(pos, top_nested_clauses);
    String query = calculateTopNestedWithOthers(top_nested_clauses, pos.max_depth);

    ASTPtr select_node;
    Tokens tokens(query.c_str(), query.c_str() + query.size());
    IParser::Pos new_pos(tokens, pos.max_depth);
    if (!ParserSelectQuery().parse(new_pos, select_node, expected))
        return false;

    auto with_node = select_node->as<ASTSelectQuery>()->with();

    auto * with_elem = with_node->children[0]->as<ASTWithElement>();

    auto sub_select = with_elem->children[0]->children[0]->children[0]->children[0];
    if (!setSubQuerySource(sub_select, node, false, false, ""))
        return false;

    node = std::move(select_node);
    return true;
}

}
