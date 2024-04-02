#pragma once

#include "IStorageSystemOneBlock.h"

namespace DB
{
class StorageSystemSchema : public IStorageSystemOneBlock
{
public:
    StorageSystemSchema(StorageID table_id_, ASTPtr query_) : IStorageSystemOneBlock(std::move(table_id_), getColumnsDescription()), query(std::move(query_)) { }
    ~StorageSystemSchema() override = default;

    void fillData(MutableColumns & res_columns, ContextPtr context, const ActionsDAG::Node *, std::vector<UInt8>) const override;
    std::string getName() const override { return "SystemSchema"; }
    static ColumnsDescription getColumnsDescription();

private:
    ASTPtr query;
};
}
