#include <Parsers/ExpressionListParsers.h>
#include <Parsers/ParserSelectQuery.h>
#include <Parsers/Kusto/ParserKQLQuery.h>
#include <Parsers/Kusto/ParserKQLProjectAway.h>
#include <format>
#include "Utilities.h"
namespace DB
{
namespace ErrorCodes
{
    extern const int SYNTAX_ERROR;
}
bool ParserKQLProjectAway::parseImpl(Pos & pos, ASTPtr & /*node*/, Expected &/* expected*/)
{
    size_t bracket_count = 0;
    auto begin_pos = pos;
    String project_away_query;
    String wildcard_column_str;
    String regular_column_str;
    std::vector<String> wildcard_columns;
    std::vector<String> regular_columns;
    ASTPtr sub_query_node;

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
            const auto column = String(begin_pos->begin, end_pos->end);
            const auto regex_column = WildcardToRegex(column);
            if (regex_column == column)
                regular_columns.push_back(column);
            else
                wildcard_columns.push_back("'" + regex_column + "'");
            begin_pos = pos;
            ++begin_pos;
        }
        ++pos;
    }
    if (wildcard_columns.empty() && regular_columns.empty())
        throw Exception("Syntax error: Missing projected away expressions", ErrorCodes::SYNTAX_ERROR);

    if (!regular_columns.empty())
    {
        if (regular_columns.size() == 1)
            regular_column_str =  regular_columns[0];
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

    String first_away = std::format("SELECT * EXCEPT {} from dummy", regular_columns.empty() ? wildcard_columns[0] : regular_column_str);

    size_t wildcard_columns_index =  regular_columns.empty() ? 1 : 0;

    for (size_t i = wildcard_columns_index; i < wildcard_columns.size(); ++i)
    {
        project_away_query = std::format("SELECT * EXCEPT {} FROM ({})", wildcard_columns[i], first_away);
        first_away = project_away_query;
    }

    if (!parseSQLQueryByString(std::make_unique<ParserSelectQuery>(), project_away_query, sub_query_node, pos.max_depth))
        return false;
  //  sub_query_node->as<ASTSelectQuery>()->setExpression(ASTSelectQuery::Expression::TABLES, std::move(node));
   // node = std::move(sub_query_node);

    return true;
}

}
