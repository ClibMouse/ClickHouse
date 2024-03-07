#include "TableFunctionGetSchema.h"

#include "TableFunctionFactory.h"

#include <Parsers/ASTFunction.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Storages/System/StorageSystemSchema.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

ColumnsDescription TableFunctionGetSchema::getActualTableStructure(ContextPtr, bool /*is_insert_query*/) const
{
    return ColumnsDescription{StorageSystemSchema::getNamesAndTypes()};
}

void TableFunctionGetSchema::parseArguments(const ASTPtr &, ContextPtr)
{
    // the same parameters are available in `executeImpl`, so we don't need to do anything here
}

StoragePtr
TableFunctionGetSchema::executeImpl(const ASTPtr & ast_function, ContextPtr, const std::string & table_name, ColumnsDescription, bool /*is_insert_query*/) const
{
    const auto * function = ast_function->as<ASTFunction>();
    if (!function)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Expected a function when parsing {}", name);

    const auto * query = function->tryGetQueryArgument();
    auto res = std::make_shared<StorageSystemSchema>(StorageID(getDatabaseName(), table_name), query->clone());
    res->startup();
    return res;
}

void registerTableFunctionGetSchema(TableFunctionFactory & factory)
{
    factory.registerFunction<TableFunctionGetSchema>({.documentation = {}, .allow_readonly = true});
}
}
