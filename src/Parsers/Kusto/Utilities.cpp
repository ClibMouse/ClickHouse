#include "Utilities.h"

#include "KustoFunctions/IParserKQLFunction.h"

#include <Parsers/ASTAsterisk.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTSubquery.h>
#include <Parsers/ASTTablesInSelectQuery.h>
namespace DB
{
String extractLiteralArgumentWithoutQuotes(const std::string & function_name, IParser::Pos & pos)
{
    ++pos;
    if (pos->type == TokenType::QuotedIdentifier || pos->type == TokenType::StringLiteral)
    {
        auto result = extractTokenWithoutQuotes(pos);
        ++pos;
        return result;
    }

    --pos;
    return IParserKQLFunction::getArgument(function_name, pos, IParserKQLFunction::ArgumentState::Raw);
}

String extractTokenWithoutQuotes(IParser::Pos & pos)
{
    const auto offset = static_cast<int>(pos->type == TokenType::QuotedIdentifier || pos->type == TokenType::StringLiteral);
    return {pos->begin + offset, pos->end - offset};
}

void setSelectAll(ASTSelectQuery & select_query)
{
    auto expression_list = std::make_shared<ASTExpressionList>();
    expression_list->children.push_back(std::make_shared<ASTAsterisk>());
    select_query.setExpression(ASTSelectQuery::Expression::SELECT, std::move(expression_list));
}

std::optional<String> wildcardToRegex(const String & wildcard)
{
    String regex;
    regex += '^';
    bool has_wildcard = false;
    for (char c : wildcard)
    {
        if (c == '*')
        {
            regex += ".*";
            has_wildcard = true;
        }
        else if (c == '?')
        {
            regex += ".";
            has_wildcard = true;
        }
        else if (c == '.' || c == '+' || c == '(' || c == ')' || c == '[' || c == ']' || c == '\\' || c == '^' || c == '$')
        {
            regex += "\\";
            regex += c;
            has_wildcard = true;
        }
        else
        {
            regex += c;
        }
    }
    regex += '$';

    if (has_wildcard)
        return regex;

    return {};
}

ASTPtr wrapInSelectWithUnion(const ASTPtr & select_query)
{
    auto select_with_union_query = std::make_shared<ASTSelectWithUnionQuery>();
    auto & list_of_selects = select_with_union_query->list_of_selects;
    list_of_selects = std::make_shared<ASTExpressionList>();
    list_of_selects->children.push_back(select_query);
    select_with_union_query->children.push_back(list_of_selects);

    return select_with_union_query;
}

void apply_subquery(ASTPtr &select_node, ASTPtr &previous_node)
{
    ASTPtr node_subquery = std::make_shared<ASTSubquery>();
    node_subquery->children.push_back(wrapInSelectWithUnion(previous_node));

    ASTPtr node_table_expr = std::make_shared<ASTTableExpression>();
    node_table_expr->as<ASTTableExpression>()->subquery = node_subquery;

    node_table_expr->children.emplace_back(node_subquery);

    ASTPtr node_table_in_select_query_emlement = std::make_shared<ASTTablesInSelectQueryElement>();
    node_table_in_select_query_emlement->as<ASTTablesInSelectQueryElement>()->table_expression = node_table_expr;

    ASTPtr tables = std::make_shared<ASTTablesInSelectQuery>();

    tables->children.emplace_back(node_table_in_select_query_emlement);
    select_node->as<ASTSelectQuery>()->setExpression(ASTSelectQuery::Expression::TABLES, std::move(tables));
}
}
