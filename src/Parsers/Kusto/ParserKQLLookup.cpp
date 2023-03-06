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

bool ParserKQLLookup::updatePipeLine(Pos pos, String & query)
{
    if (!ParserKeyword("lookup").ignore(pos))
        return false;

    if (pos->isEnd() || pos->type == TokenType::PipeMark || pos->type == TokenType::Semicolon)
        return false;

    String join_kind = "kind=leftouter";
    ParserKeyword s_kind("kind");
    ParserToken equals(TokenType::Equals);
    auto end_pos = pos;

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
    while (!pos->isEnd() && pos->type != TokenType::Semicolon)
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

    query = std::format("join {} {} ", join_kind, right_expr);

    return true;
}

bool ParserKQLLookup::parseImpl(Pos & /*pos*/, ASTPtr & /*node*/, Expected & /*expected*/) //(Pos & pos, ASTPtr & node, Expected & expected)
{
    return true;
}

}
