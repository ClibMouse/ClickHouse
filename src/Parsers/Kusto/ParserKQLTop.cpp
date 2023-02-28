#include <Parsers/ExpressionListParsers.h>
#include <Parsers/Kusto/ParserKQLQuery.h>
#include <Parsers/Kusto/ParserKQLTop.h>

#include <format>

namespace DB
{
namespace ErrorCodes
{
    extern const int SYNTAX_ERROR;
}

bool ParserKQLTop::parseImpl(Pos & /*pos*/, ASTPtr & /*node*/, Expected & /*expected*/)
{
    return true;
}

bool ParserKQLTop::updatePipeLine (OperationsPos & operations, String & query)
{
    Pos pos = operations.back().second;

    if (pos->isEnd() || pos->type == TokenType::PipeMark || pos->type == TokenType::Semicolon)
        throw Exception(ErrorCodes::SYNTAX_ERROR, "Syntax error near top operator");

    Pos start_pos = operations.front().second;
    Pos end_pos = pos;
    --end_pos;
    --end_pos;

    String prev_query(start_pos->begin, end_pos->end);

    String limit_expr, sort_expr;
    start_pos = pos;
    end_pos = pos;
    while (!pos->isEnd() && pos->type != TokenType::PipeMark && pos->type != TokenType::Semicolon)
    {
        if (String(pos->begin, pos->end) == "by")
        {
            auto limt_end_pos = pos;
            --limt_end_pos;
            limit_expr = String(start_pos->begin, limt_end_pos->end);
            start_pos = pos;
            ++start_pos;
        }
        end_pos =  pos;
        ++pos;
    }
    sort_expr = (start_pos <= end_pos) ? String(start_pos->begin, end_pos->end) : "";
    if (limit_expr.empty() || sort_expr.empty())
        throw Exception(ErrorCodes::SYNTAX_ERROR, "top operator need a by clause");

    query = std::format("{} sort by {} | take {}", prev_query, sort_expr, limit_expr);

    return true;
}

}
