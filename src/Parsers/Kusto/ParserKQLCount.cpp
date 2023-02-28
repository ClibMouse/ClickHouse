#include <Parsers/ExpressionListParsers.h>
#include <Parsers/Kusto/ParserKQLQuery.h>
#include <Parsers/Kusto/ParserKQLCount.h>
namespace DB
{
namespace ErrorCodes
{
    extern const int SYNTAX_ERROR;
}

bool ParserKQLCount::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    if (!pos->isEnd() && pos->type != TokenType::PipeMark && pos->type != TokenType::Semicolon)
        throw Exception(ErrorCodes::SYNTAX_ERROR, "Syntax error near count operator");

    ASTPtr select_expression_list;
    String converted_columns =  getExprFromToken("Count = count()", pos.max_depth);

    Tokens token_converted_columns(converted_columns.c_str(), converted_columns.c_str() + converted_columns.size());
    IParser::Pos pos_converted_columns(token_converted_columns, pos.max_depth);

    if (!ParserNotEmptyExpressionList(true).parse(pos_converted_columns, select_expression_list, expected))
        return false;

    node->as<ASTSelectQuery>()->setExpression(ASTSelectQuery::Expression::SELECT, std::move(select_expression_list));

    return true;
}

}
