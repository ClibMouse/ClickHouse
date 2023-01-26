#include <format>
#include <Parsers/ExpressionListParsers.h>
#include <Parsers/Kusto/ParserKQLQuery.h>
#include <Parsers/Kusto/ParserKQLRange.h>
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
    String columnName, start, stop, step;
    auto start_pos = pos;
    auto end_pos = pos;
    while (!pos->isEnd() && pos->type != TokenType::PipeMark && pos->type != TokenType::Semicolon)
    {
        if (String(pos->begin, pos->end) == "from")
        {
            end_pos = pos;
            --end_pos;
            if (end_pos < start_pos)
                throw Exception("Missing columnName for range operator", ErrorCodes::SYNTAX_ERROR);

            columnName = String(start_pos->begin, end_pos->end);
            start_pos = pos;
            ++start_pos;
        }
        if (String(pos->begin, pos->end) == "to")
        {
            if (columnName.empty())
                throw Exception("Missing `from` for range operator", ErrorCodes::SYNTAX_ERROR);
            end_pos = pos;
            --end_pos;
            if (end_pos < start_pos)
                throw Exception("Missing start expression for range operator", ErrorCodes::SYNTAX_ERROR);
            start = String(start_pos->begin, end_pos->end);
            start_pos = pos;
            ++start_pos;
        }
        if (String(pos->begin, pos->end) == "step")
        {
            if (columnName.empty())
                throw Exception("Missing `from` for range operator", ErrorCodes::SYNTAX_ERROR);
            if (start.empty())
                throw Exception("Missing 'to' for range operator", ErrorCodes::SYNTAX_ERROR);

            end_pos = pos;
            --end_pos;
            if (end_pos < start_pos)
                throw Exception("Missing stop expression for range operator", ErrorCodes::SYNTAX_ERROR);

            stop = String(start_pos->begin, end_pos->end);
            start_pos = pos;
            ++start_pos;
        }
        ++pos;
    }

    if (columnName.empty() || start.empty() || stop.empty())
        throw Exception("Missing required expression for range operator", ErrorCodes::SYNTAX_ERROR);

    end_pos = pos;
    --end_pos;
    if (end_pos < start_pos)
        throw Exception("Missing step expression for range operator", ErrorCodes::SYNTAX_ERROR);

    step = String(start_pos->begin, end_pos->end);

    columnName = getExprFromToken(columnName, pos.max_depth);
    start = getExprFromToken(start, pos.max_depth);
    stop = getExprFromToken(stop, pos.max_depth);
    step = getExprFromToken(step, pos.max_depth);
    String query = std::format("SELECT * FROM (SELECT kql_range({0}, {1},{2}) AS {3}) ARRAY JOIN {3}", start, stop, step, columnName);

    if (!parseSQLQueryByString(std::make_unique<ParserSelectQuery>(), query, select_node, pos.max_depth))
        return false;
    node = std::move(select_node);

    return true;
}

}
