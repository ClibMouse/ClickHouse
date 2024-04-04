#include <format>
#include <Parsers/ExpressionListParsers.h>
#include <Parsers/Kusto/ParserKQLQuery.h>
#include <Parsers/Kusto/ParserKQLRange.h>
#include <Parsers/Kusto/Utilities.h>
#include <Parsers/ParserSelectQuery.h>
namespace DB
{

namespace ErrorCodes
{
    extern const int SYNTAX_ERROR;
}

bool ParserKQLRange::parseImpl(Pos & pos, ASTPtr & node, Expected & /*expected*/)
{
    ASTPtr select_node;
    String column_name, start, stop, step;
    auto start_pos = pos;
    auto end_pos = pos;
    BracketCount bracket_count;
    while (isValidKQLPos(pos))
    {
        bracket_count.count(pos);
        if ((pos->type == TokenType::PipeMark || pos->type == TokenType::Semicolon) && bracket_count.isZero())
            break;

        if (String(pos->begin, pos->end) == "from" && bracket_count.isZero())
        {
            end_pos = pos;
            --end_pos;
            if (end_pos < start_pos)
                throw Exception(ErrorCodes::SYNTAX_ERROR, "Missing column name for range operator");

            column_name = String(start_pos->begin, end_pos->end);
            start_pos = pos;
            ++start_pos;
        }
        if (String(pos->begin, pos->end) == "to" && bracket_count.isZero())
        {
            if (column_name.empty())
                throw Exception(ErrorCodes::SYNTAX_ERROR, "Missing `from` for range operator");
            end_pos = pos;
            --end_pos;
            if (end_pos < start_pos)
                throw Exception(ErrorCodes::SYNTAX_ERROR, "Missing start expression for range operator");
            start = String(start_pos->begin, end_pos->end);
            start_pos = pos;
            ++start_pos;
        }
        if (String(pos->begin, pos->end) == "step" && bracket_count.isZero())
        {
            if (column_name.empty())
                throw Exception(ErrorCodes::SYNTAX_ERROR, "Missing `from` for range operator");
            if (start.empty())
                throw Exception(ErrorCodes::SYNTAX_ERROR, "Missing 'to' for range operator");

            end_pos = pos;
            --end_pos;
            if (end_pos < start_pos)
                throw Exception(ErrorCodes::SYNTAX_ERROR, "Missing stop expression for range operator");

            stop = String(start_pos->begin, end_pos->end);
            start_pos = pos;
            ++start_pos;
        }
        ++pos;
    }

    if (column_name.empty() || start.empty() || stop.empty())
        throw Exception(ErrorCodes::SYNTAX_ERROR, "Missing required expression for range operator");

    end_pos = pos;
    --end_pos;
    if (end_pos < start_pos)
        throw Exception(ErrorCodes::SYNTAX_ERROR, "Missing step expression for range operator");

    step = String(start_pos->begin, end_pos->end);

    column_name = getExprFromToken(column_name, pos.max_depth, pos.max_backtracks);
    start = getExprFromToken(start, pos.max_depth, pos.max_backtracks);
    stop = getExprFromToken(stop, pos.max_depth, pos.max_backtracks);
    step = getExprFromToken(step, pos.max_depth, pos.max_backtracks);
    String query = std::format("SELECT * FROM (SELECT kql_range({0}, {1},{2}) AS {3}) ARRAY JOIN {3}", start, stop, step, column_name);

    if (!parseSQLQueryByString(std::make_unique<ParserSelectQuery>(), query, select_node, pos.max_depth, pos.max_backtracks))
        return false;
    node = std::move(select_node);

    return true;
}

}
