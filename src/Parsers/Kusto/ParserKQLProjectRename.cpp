#include "ParserKQLProjectRename.h"

#include <Parsers/ASTAsterisk.h>
#include <Parsers/ASTColumnsTransformers.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ExpressionListParsers.h>

#include <format>

namespace DB
{
bool ParserKQLProjectRename::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    const auto projectrename_expr = getExprFromToken(pos);
    Tokens ntokens(projectrename_expr.c_str(), projectrename_expr.c_str() + projectrename_expr.size());
    IParser::Pos npos(ntokens, pos.max_depth);

    ASTPtr expression_list;
    if (!ParserNotEmptyExpressionList(false).parse(npos, expression_list, expected) || !npos->isEnd())
        return false;
    
    std::ranges::for_each(
        expression_list->children,
        [this](const ASTPtr & expression)
        {
            if (const auto alias = expression->tryGetAlias(); !alias.empty())
                return;

            expression->setAlias(kql_context.nextDefaultColumnName());
        });

    auto asterisk = std::make_shared<ASTAsterisk>();
    asterisk->transformers = std::make_shared<ASTColumnsTransformerList>();
    const auto & columns_except_transformer
        = asterisk->children.emplace_back(asterisk->transformers)->children.emplace_back(std::make_shared<ASTColumnsExceptTransformer>());

    std::ranges::transform(
        expression_list->children,
        std::back_inserter(columns_except_transformer->children),
        [](const ASTPtr & child) { return std::make_shared<ASTIdentifier>(child->getID().substr(11)); });

    expression_list->children.insert(expression_list->children.cbegin(), std::move(asterisk));

    node->as<ASTSelectQuery>()->setExpression(ASTSelectQuery::Expression::SELECT, std::move(expression_list));
    return true;
}
}
