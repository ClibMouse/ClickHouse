#include <Parsers/ExpressionListParsers.h>
#include <Parsers/Kusto/ParserKQLQuery.h>
#include <Parsers/Kusto/ParserKQLTopHitter.h>

#include <format>

namespace DB
{
namespace ErrorCodes
{
    extern const int SYNTAX_ERROR;
}

bool ParserKQLTopHitters::parseImpl(Pos & /*pos*/, ASTPtr & /*node*/, Expected & /*expected*/)
{
    return true;
}

bool ParserKQLTopHitters::updatePipeLine (OperationsPos & operations, String & query)
{
    Pos pos = operations.back().second;

    if (pos->isEnd() || pos->type == TokenType::PipeMark || pos->type == TokenType::Semicolon)
        throw Exception(ErrorCodes::SYNTAX_ERROR, "Syntax error near top-hitters operator");

    Pos start_pos = operations.front().second;
    Pos end_pos = pos;
    for (auto i = 0; i < 4; ++i)
        --end_pos;

    String prev_query(start_pos->begin, end_pos->end);

    String number_of_values, value_expression, summing_expression;
    start_pos = pos;
    end_pos = pos;
    while (!pos->isEnd() && pos->type != TokenType::PipeMark && pos->type != TokenType::Semicolon)
    {
        if (String(pos->begin, pos->end) == "of")
        {
            auto number_end_pos = pos;
            --number_end_pos;
            number_of_values = String(start_pos->begin, number_end_pos->end);
            start_pos = pos;
            ++start_pos;
        }

        if (String(pos->begin, pos->end) == "by")
        {
            auto expr_end_pos = pos;
            --expr_end_pos;
            value_expression = String(start_pos->begin, expr_end_pos->end);
            start_pos = pos;
            ++start_pos;
        }
        end_pos =  pos;
        ++pos;
    }

    if (value_expression.empty())
        value_expression = (start_pos <= end_pos) ? String(start_pos->begin, end_pos->end) : "";
    else
        summing_expression = (start_pos <= end_pos) ? String(start_pos->begin, end_pos->end) : "";

    if (number_of_values.empty() || value_expression.empty())
        throw Exception(ErrorCodes::SYNTAX_ERROR, "top-hitter operator need a ValueExpression");

    if (summing_expression.empty())
        query = std::format("{0} summarize approximate_count_{1} = count() by {1} | sort by approximate_count_{1} desc | take {2} ", prev_query, value_expression, number_of_values);
    else
        query = std::format("{0} summarize approximate_sum_{1} = sum({1}) by {2} | sort by approximate_sum_{1} desc | take {3}", prev_query, summing_expression, value_expression, number_of_values);

    return true;
}

}
