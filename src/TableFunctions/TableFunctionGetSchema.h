#pragma once

#include "ITableFunction.h"

namespace DB
{
class TableFunctionGetSchema : public ITableFunction
{
public:
    static constexpr auto name = "getschema";

    ~TableFunctionGetSchema() override = default;

    ColumnsDescription getActualTableStructure(ContextPtr context, bool is_insert_query) const override;
    std::string getName() const override { return name; }
    bool hasStaticStructure() const override { return true; }

    void parseArguments(const ASTPtr & ast_function, ContextPtr context) override;

private:
    const char * getStorageTypeName() const override { return "GetSchema"; }

    StoragePtr executeImpl(
        const ASTPtr & ast_function, ContextPtr context, const std::string & table_name, ColumnsDescription cached_columns, bool is_insert_query) const override;
};

class TableFunctionFactory;
void registerTableFunctionGetSchema(TableFunctionFactory & factory);
}
