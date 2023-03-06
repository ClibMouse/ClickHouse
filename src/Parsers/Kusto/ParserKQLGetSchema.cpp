#include "ParserKQLGetSchema.h"

#include "Utilities.h"

#include <Parsers/ASTFunction.h>

namespace DB
{
bool ParserKQLGetSchema::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    if (const auto & token = pos->type; token != TokenType::EndOfStream && token != TokenType::PipeMark && token != TokenType::Semicolon)
    {
        expected.add(pos, "end of query or next pipe");
        return false;
    }

    auto * select_query = node->as<ASTSelectQuery>();
    if (!select_query)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected node: {}", node->getID());

    if (!select_query->select())
        setSelectAll(*select_query);

    auto enclosing_query = std::make_shared<ASTSelectQuery>();
    ASTPtr getschema_function = makeASTFunction("getschema", wrapInSelectWithUnion(node));
    enclosing_query->addTableFunction(getschema_function);

    node = std::move(enclosing_query);
    return true;
}
}
