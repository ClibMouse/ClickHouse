#include <format>
#include <Parsers/ExpressionListParsers.h>
#include <Parsers/Kusto/ParserKQLProjectAway.h>
#include <Parsers/Kusto/ParserKQLQuery.h>
#include <Parsers/ParserSelectQuery.h>
#include <Parsers/ParserTablesInSelectQuery.h>
#include "Utilities.h"
namespace DB
{
namespace ErrorCodes
{
    extern const int SYNTAX_ERROR;
}
bool ParserKQLProjectAway::parseImpl(Pos & pos, ASTPtr & node, Expected & /*expected*/)
{
    size_t bracket_count = 0;
    auto begin_pos = pos;
    String regular_column_str;
    std::vector<String> wildcard_columns;
    std::vector<String> regular_columns;
    ASTPtr sub_query_node;

    auto append_columns = [&regular_columns, &wildcard_columns](Pos & begin, Pos & end)
    {
        const auto column = String(begin->begin, end->end);
        const auto regex_column = WildcardToRegex(column);
        if (regex_column == column)
            regular_columns.push_back(column);
        else
            wildcard_columns.push_back("'" + regex_column + "'");
    };

    while (!pos->isEnd() && pos->type != TokenType::PipeMark && pos->type != TokenType::Semicolon)
    {
        if (pos->type == TokenType::OpeningRoundBracket)
            ++bracket_count;

        if (pos->type == TokenType::ClosingRoundBracket)
            --bracket_count;

        if (bracket_count == 0 and pos->type == TokenType::Comma)
        {
            auto end_pos = pos;
            --end_pos;
            append_columns(begin_pos, end_pos);
            begin_pos = pos;
            ++begin_pos;
        }
        ++pos;
    }

    --pos;
    append_columns(begin_pos, pos);

    if (wildcard_columns.empty() && regular_columns.empty())
        throw Exception("Syntax error: Missing projected away expressions", ErrorCodes::SYNTAX_ERROR);

    if (!regular_columns.empty())
    {
        if (regular_columns.size() == 1)
            regular_column_str = regular_columns[0];
        else
        {
            regular_column_str = "(" + regular_columns[0];
            for (size_t i = 1; i < regular_columns.size(); ++i)
            {
                regular_column_str += "," + regular_columns[i];
            }
            regular_column_str += ")";
        }
    }

    size_t wildcard_columns_index = regular_columns.empty() ? 1 : 0;

    for (size_t i = wildcard_columns_index; i < wildcard_columns.size(); ++i)
    {
        String project_away_query = std::format("(SELECT * EXCEPT {} FROM dummy_input)", wildcard_columns[i]);
        if (!parseSQLQueryByString(std::make_unique<ParserTablesInSelectQuery>(), project_away_query, sub_query_node, pos.max_depth))
            return false;
        if (!setSubQuerySource(sub_query_node, node, true, i != wildcard_columns_index))
            return false;
        node = std::move(sub_query_node);
    }

    String last_away = std::format("SELECT * EXCEPT {} from dummy", regular_columns.empty() ? wildcard_columns[0] : regular_column_str);

    if (!parseSQLQueryByString(std::make_unique<ParserSelectQuery>(), last_away, sub_query_node, pos.max_depth))
        return false;
    if (wildcard_columns_index < wildcard_columns.size())
        sub_query_node->as<ASTSelectQuery>()->setExpression(ASTSelectQuery::Expression::TABLES, std::move(node));
    else
    {
        if (!setSubQuerySource(sub_query_node, node, false, false))
            return false;
    }
    node = std::move(sub_query_node);

    return true;
}

}
