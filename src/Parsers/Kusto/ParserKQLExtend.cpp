#include "ParserKQLExtend.h"

#include <Parsers/ASTAsterisk.h>
#include <Parsers/ASTColumnsTransformers.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ExpressionListParsers.h>
#include <Parsers/Kusto/Utilities.h>

#include <format>

namespace DB
{
bool ParserKQLExtend::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    const auto extend_expr = getExprFromToken(pos);
    Tokens ntokens(extend_expr.c_str(), extend_expr.c_str() + extend_expr.size());
    IParser::Pos npos(ntokens, pos.max_depth);

    //TODO: Clean up.
    // String alias;

    // auto apply_alias = [&]
    // {
    //     if (alias.empty())
    //     {
    //         alias = std::format("Column{}", new_column_index);
    //         ++new_column_index;
    //         new_extend_str += " AS";
    //     }
    //     else
    //         except_str = except_str.empty() ? " except " + alias : except_str + " except " + alias;

    //     new_extend_str = new_extend_str + " " + alias;

    //     alias.clear();
    // };

    // int32_t round_bracket_count = 0;
    // int32_t square_bracket_count = 0;
    // while (isValidKQLPos(npos))
    // {
    //     if (npos->type == TokenType::OpeningRoundBracket)
    //         ++round_bracket_count;
    //     if (npos->type == TokenType::OpeningSquareBracket)
    //         ++square_bracket_count;
    //     if (npos->type == TokenType::ClosingRoundBracket)
    //         --round_bracket_count;
    //     if (npos->type == TokenType::ClosingSquareBracket)
    //         --square_bracket_count;

    //     auto expr = String(npos->begin, npos->end);
    //     if (expr == "AS")
    //     {
    //         ++npos;
    //         alias = String(npos->begin, npos->end);
    //     }

    //     if (npos->type == TokenType::Comma && square_bracket_count == 0 && round_bracket_count == 0)
    //     {
    //         apply_alias();
    //         new_extend_str += ", ";
    //     }
    //     else
    //         new_extend_str = new_extend_str.empty() ? expr : new_extend_str + " " + expr;

    //     ++npos;
    // }
    // apply_alias();

    // String expr = std::format("SELECT * {}, {} from prev", except_str, new_extend_str);
    // Tokens tokens(expr.c_str(), expr.c_str() + expr.size());
    // IParser::Pos new_pos(tokens, pos.max_depth);

    // if (!ParserSelectQuery().parse(new_pos, select_query, expected))
    //     return false;
    // if (!setSubQuerySource(select_query, node, false, false))
    //     return false;

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
        [](const ASTPtr & child) { return std::make_shared<ASTIdentifier>(child->getAliasOrColumnName()); });

    expression_list->children.insert(expression_list->children.cbegin(), std::move(asterisk));

    node->as<ASTSelectQuery>()->setExpression(ASTSelectQuery::Expression::SELECT, std::move(expression_list));
    return true;
}
}
