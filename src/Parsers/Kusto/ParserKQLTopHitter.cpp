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

bool ParserKQLTopHitters::updatePipeLine(Pos pos, String & query)
{
    if (!ParserSequence("top-hitters").ignore(pos))
        return false;

    if (pos->isEnd() || pos->type == TokenType::PipeMark || pos->type == TokenType::Semicolon)
        throw Exception(ErrorCodes::SYNTAX_ERROR, "Syntax error near top-hitters operator");

    String number_of_values, value_expression, summing_expression;
    auto start_pos = pos;
    auto end_pos = pos;
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
        query = std::format("summarize approximate_count_{0} = count() by {0} | sort by approximate_count_{0} desc | take {1} ", value_expression, number_of_values);
    else
        query = std::format("summarize approximate_sum_{0} = sum({0}) by {1} | sort by approximate_sum_{0} desc | take {2}", summing_expression, value_expression, number_of_values);

    return true;
}

}
