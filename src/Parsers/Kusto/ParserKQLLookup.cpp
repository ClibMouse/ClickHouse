#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTSubquery.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ExpressionListParsers.h>
#include <Parsers/IParserBase.h>
#include <Parsers/Kusto/ParserKQLLookup.h>
#include <Parsers/Kusto/ParserKQLQuery.h>
#include <Parsers/Kusto/Utilities.h>
#include <Parsers/ParserSelectQuery.h>
#include <Parsers/ParserTablesInSelectQuery.h>
#include <Common/StringUtils/StringUtils.h>

#include <format>

namespace DB
{

namespace ErrorCodes
{
    extern const int SYNTAX_ERROR;
}

bool ParserKQLLookup::updatePipeLine(OperationsPos & operations, String & query)
{
    Pos pos = operations.back().second;

    if (!isValidKQLPos(pos) || pos->type == TokenType::PipeMark || pos->type == TokenType::Semicolon)
        throw Exception(ErrorCodes::SYNTAX_ERROR, "Syntax error near lookup operator");

    Pos start_pos = operations.front().second;
    Pos end_pos = pos;
    --end_pos;
    --end_pos;

    String prev_query(start_pos->begin, end_pos->end);

    String join_kind = "kind=leftouter";
    ParserKeyword s_kind("kind");
    ParserToken equals(TokenType::Equals);
    start_pos = pos;
    end_pos = pos;

    if (s_kind.ignore(pos))
    {
        if (!equals.ignore(pos))
            throw Exception(ErrorCodes::SYNTAX_ERROR, "Invalid kind for lookup operator");

        if (ParserKeyword("leftouter").ignore(pos))
            join_kind = "kind=leftouter";
        else if (ParserKeyword("inner").ignore(pos))
            join_kind = "kind=inner";
        else
            throw Exception(ErrorCodes::SYNTAX_ERROR, "Invalid value of kind for lookup operator");
    }
    Pos right_table_start_pos = pos;

    size_t paren_count = 0;
    while (isValidKQLPos(pos) && pos->type != TokenType::Semicolon)
    {
        if (pos->type == TokenType::OpeningRoundBracket)
            ++paren_count;
        if (pos->type == TokenType::ClosingRoundBracket)
            --paren_count;
        if (pos->type == TokenType::PipeMark && paren_count == 0)
            break;
        end_pos = pos;
        ++pos;
    }

    String right_expr = (right_table_start_pos <= end_pos) ? String(right_table_start_pos->begin, end_pos->end) : "";
    if (right_expr.empty())
        throw Exception(ErrorCodes::SYNTAX_ERROR, "lookup operator need right table");

    query = std::format("{} join {} {} ", prev_query, join_kind, right_expr);

    return true;
}

bool ParserKQLLookup::parseImpl(Pos & /*pos*/, ASTPtr & /*node*/, Expected & /*expected*/) //(Pos & pos, ASTPtr & node, Expected & expected)
{
    return true;
}

}
