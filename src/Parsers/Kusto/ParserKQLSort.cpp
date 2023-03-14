#include <format>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTOrderByElement.h>
#include <Parsers/ExpressionListParsers.h>
#include <Parsers/IParserBase.h>
#include <Parsers/Kusto/ParserKQLQuery.h>
#include <Parsers/Kusto/ParserKQLSort.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int SYNTAX_ERROR;
}

bool ParserKQLSort::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    String order_list_str;
    ParserOrderByExpressionList order_list;
    ASTPtr order_expression_list;

    auto validate_column = [&](Pos & pos1, Pos & pos2)
    {
        if (pos2->type == TokenType::BareWord && pos1 != pos2)
            throw Exception(
                ErrorCodes::SYNTAX_ERROR,
                "{} does not refer to any known column, table, variable or function",
                String(pos2->begin, pos2->end));

        return String(pos1->begin, pos2->end);
    };

    auto format_sort_expr = [&](const Pos & pos1, const Pos & pos2)
    {
        auto start_pos = pos1;
        auto end_pos = pos2;
        String column_expr, sort_dir, nulls_position;
        auto tmp_pos = start_pos;
        while (tmp_pos < end_pos)
        {
            String tmp(tmp_pos->begin, tmp_pos->end);
            if (tmp == "desc" || tmp == "asc")
            {
                if (!sort_dir.empty() || !nulls_position.empty())
                    throw Exception(ErrorCodes::SYNTAX_ERROR, "The incomplete fragment is unexpected");
                --tmp_pos;
                column_expr = validate_column(start_pos, tmp_pos);
                sort_dir = tmp;
                ++tmp_pos;
            }
            if (tmp == "nulls")
            {
                if (!nulls_position.empty())
                    throw Exception(ErrorCodes::SYNTAX_ERROR, "The incomplete fragment is unexpected");
                auto nulls_pos = tmp_pos;
                ++tmp_pos;
                tmp = String(tmp_pos->begin, tmp_pos->end);
                if (tmp_pos->isEnd() || (tmp != "first" && tmp != "last"))
                    throw Exception(ErrorCodes::SYNTAX_ERROR, "Invalid nulls postion of sort operator");

                nulls_position = "nulls " + tmp;
                if (column_expr.empty())
                {
                    --nulls_pos;
                    column_expr = validate_column(start_pos, nulls_pos);
                }
            }

            ++tmp_pos;
        }
        --end_pos;
        if (column_expr.empty())
            column_expr = validate_column(start_pos, end_pos);

        if (sort_dir.empty())
            sort_dir = "desc";
        if (nulls_position.empty())
            nulls_position = sort_dir == "desc" ? "nulls last" : "nulls first";
        return std::format("{} {} {}", getExprFromToken(column_expr, pos.max_depth), sort_dir, nulls_position);
    };

    auto paren_count = 0;
    auto begin = pos;
    while (!pos->isEnd() && pos->type != TokenType::PipeMark && pos->type != TokenType::Semicolon)
    {
        if (pos->type == TokenType::ClosingRoundBracket)
            --paren_count;
        if (pos->type == TokenType::OpeningRoundBracket)
            ++paren_count;
        if (pos->type == TokenType::Comma && paren_count == 0)
        {
            auto single_sort_expr = format_sort_expr(begin, pos);
            order_list_str = order_list_str.empty() ? single_sort_expr : order_list_str + "," + single_sort_expr;
            begin = pos;
            ++begin;
        }
        ++pos;
    }

    auto single_sort_expr = format_sort_expr(begin, pos);
    order_list_str = order_list_str.empty() ? single_sort_expr : order_list_str + "," + single_sort_expr;

    Tokens tokens(order_list_str.c_str(), order_list_str.c_str() + order_list_str.size());
    IParser::Pos new_pos(tokens, pos.max_depth);

    if (!order_list.parse(new_pos, order_expression_list, expected))
        return false;

    node->as<ASTSelectQuery>()->setExpression(ASTSelectQuery::Expression::ORDER_BY, std::move(order_expression_list));
    return true;
}

}
