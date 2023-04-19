#include "ParserKQLProject.h"

#include <Parsers/ASTAsterisk.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ExpressionListParsers.h>

namespace DB
{
bool ParserKQLProject::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    const auto expr = getExprFromToken(pos);
    Tokens tokens(expr.c_str(), expr.c_str() + expr.size());
    IParser::Pos new_pos(tokens, pos.max_depth);

    ASTPtr select_expression_list;
    if (!ParserNotEmptyExpressionList(false).parse(new_pos, select_expression_list, expected))
        return false;

    std::ranges::for_each(
        select_expression_list->children,
        [this](const ASTPtr & expression)
        {
            if (expression->as<ASTAsterisk>() || expression->as<ASTIdentifier>())
                return;

            if (const auto alias = expression->tryGetAlias(); !alias.empty())
                return;

            expression->setAlias(kql_context.nextDefaultColumnName());
        });

    node->as<ASTSelectQuery>()->setExpression(ASTSelectQuery::Expression::SELECT, std::move(select_expression_list));

    return true;
}
}
