#include <Parsers/ExpressionListParsers.h>
#include <Parsers/Kusto/ParserKQLPrint.h>

#include <format>

namespace DB
{

bool ParserKQLPrint::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    const auto expr = getExprFromToken(pos);

    Tokens tokens(expr.c_str(), expr.c_str() + expr.size());
    IParser::Pos new_pos(tokens, pos.max_depth, pos.max_backtracks);

    ASTPtr select_expression_list;
    if (!ParserNotEmptyExpressionList(true).parse(new_pos, select_expression_list, expected))
        return false;

    int column_index = 0;
    std::ranges::for_each(
        select_expression_list->children,
        [&column_index](const ASTPtr & expression)
        {
            if (const auto alias = expression->tryGetAlias(); alias.empty())
                expression->setAlias(std::format("print_{}", column_index));

            ++column_index;
        });

    node->as<ASTSelectQuery>()->setExpression(ASTSelectQuery::Expression::SELECT, std::move(select_expression_list));
    return true;
}

}
