#include "StorageSystemSchema.h"

#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionHelpers.h>
#include <Interpreters/InterpreterSelectWithUnionQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/Kusto/KQLDataType.h>

namespace
{
std::string toKQLDataTypeName(const DB::DataTypePtr & data_type)
{
    const auto nested_type = DB::removeNullable(data_type);
    const auto kql_data_type = DB::toKQLDataType(nested_type->getTypeId(), DB::KQLScope::Column);
    return toString(kql_data_type);
}
}

namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

NamesAndTypesList StorageSystemSchema::getNamesAndTypes()
{
    return {
        {"ColumnName", std::make_shared<DataTypeString>()},
        {"ColumnOrdinal", std::make_shared<DataTypeInt32>()},
        {"DataType", std::make_shared<DataTypeString>()},
        {"ColumnType", std::make_shared<DataTypeString>()}};
}

void StorageSystemSchema::fillData(MutableColumns & res_columns, ContextPtr context, const SelectQueryInfo &) const
{
    if (res_columns.size() != 4)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected number of result columns when trying to fill {}", getName());

    const auto & column_names = res_columns[0];
    const auto & column_ordinals = res_columns[1];
    const auto & column_data_types = res_columns[2];
    const auto & column_types = res_columns[3];

    const auto & dialect = context->getSettingsRef().dialect;
    const auto is_kql = dialect == Dialect::kusto;

    const auto sample_block = InterpreterSelectWithUnionQuery::getSampleBlock(query, context);
    const auto & names_and_types = sample_block.getNamesAndTypes();
    for (int i = 0; i < std::ssize(names_and_types); ++i)
    {
        const auto & name_and_type = names_and_types[i];
        column_names->insert(toField(name_and_type.name));
        column_ordinals->insert(toField(i));

        const auto & type = name_and_type.type;
        const auto & type_name = type->getName();
        column_data_types->insert(toField(type_name));
        column_types->insert(toField(is_kql ? toKQLDataTypeName(type) : type_name));
    }
}
}
