#include "ParserKQLExtend.h"

#include <Parsers/ASTAsterisk.h>
#include <Parsers/ASTColumnsTransformers.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ExpressionListParsers.h>

#include <format>

namespace DB
{
bool ParserKQLExtend::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    const auto extend_expr = getExprFromToken(pos);
    Tokens ntokens(extend_expr.c_str(), extend_expr.c_str() + extend_expr.size());
    IParser::Pos npos(ntokens, pos.max_depth);

    ASTPtr expression_list;
    if (!ParserNotEmptyExpressionList(false).parse(npos, expression_list, expected) || !npos->isEnd())
        return false;

    int column_index = 1;
    std::ranges::for_each(
        expression_list->children,
        [&column_index](const ASTPtr & expression)
        {
            if (const auto alias = expression->tryGetAlias(); !alias.empty())
                return;

            expression->setAlias(std::format("Column{}", column_index));
            ++column_index;
        });

    auto asterisk = std::make_shared<ASTAsterisk>();
    asterisk->transformers = std::make_shared<ASTColumnsTransformerList>();
    const auto & columns_except_transformer
        = asterisk->children.emplace_back(asterisk->transformers)->children.emplace_back(std::make_shared<ASTColumnsExceptTransformer>());

    std::ranges::transform(
        expression_list->children,
        std::back_inserter(columns_except_transformer->children),
        [](const ASTPtr & child) { return std::make_shared<ASTIdentifier>(child->getAliasOrColumnName()); });

    expression_list->children.insert(expression_list->children.cbegin(), std::move(asterisk));

    node->as<ASTSelectQuery>()->setExpression(ASTSelectQuery::Expression::SELECT, std::move(expression_list));
    return true;
}
}
