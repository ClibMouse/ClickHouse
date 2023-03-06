#pragma once

#include "IStorageSystemOneBlock.h"

namespace DB
{
class StorageSystemSchema : public IStorageSystemOneBlock<StorageSystemSchema>
{
public:
    static NamesAndTypesList getNamesAndTypes();

    StorageSystemSchema(StorageID table_id_, ASTPtr query_) : IStorageSystemOneBlock(std::move(table_id_)), query(std::move(query_)) { }
    ~StorageSystemSchema() override = default;

    void fillData(MutableColumns & res_columns, ContextPtr context, const SelectQueryInfo & query_info) const override;
    std::string getName() const override { return "SystemSchema"; }

private:
    ASTPtr query;
};
}
