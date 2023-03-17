#include "KQLDataType.h"

#include <Common/Exception.h>

#include <magic_enum.hpp>

#include <algorithm>

namespace DB::ErrorCodes
{
extern const int LOGICAL_ERROR;
extern const int UNKNOWN_TYPE;
}

namespace
{
class KQLScopeToType
{
public:
    explicit KQLScopeToType(DB::KQLDataType column_type_) : KQLScopeToType(column_type_, column_type_) { }
    KQLScopeToType(DB::KQLDataType column_type_, DB::KQLDataType row_type_) : column_type(column_type_), row_type(row_type_) { }

    DB::KQLDataType getType(DB::KQLScope scope) const;

private:
    DB::KQLDataType column_type;
    DB::KQLDataType row_type;
};

DB::KQLDataType KQLScopeToType::getType(const DB::KQLScope scope) const
{
    if (scope == DB::KQLScope::Column)
        return column_type;
    else if (scope == DB::KQLScope::Row)
        return row_type;

    throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Unexpected KQL scope: {}", magic_enum::enum_name(scope));
}

const std::unordered_map<DB::TypeIndex, KQLScopeToType> CLICKHOUSE_TO_KQL_TYPE{
    {DB::TypeIndex::AggregateFunction, KQLScopeToType(DB::KQLDataType::Invalid)},
    {DB::TypeIndex::Array, KQLScopeToType(DB::KQLDataType::Dynamic, DB::KQLDataType::Array)},
    {DB::TypeIndex::Date, KQLScopeToType(DB::KQLDataType::Invalid)},
    {DB::TypeIndex::Date32, KQLScopeToType(DB::KQLDataType::Invalid)},
    {DB::TypeIndex::DateTime, KQLScopeToType(DB::KQLDataType::Invalid)},
    {DB::TypeIndex::DateTime64, KQLScopeToType(DB::KQLDataType::DateTime)},
    {DB::TypeIndex::Decimal32, KQLScopeToType(DB::KQLDataType::Decimal)},
    {DB::TypeIndex::Decimal64, KQLScopeToType(DB::KQLDataType::Decimal)},
    {DB::TypeIndex::Decimal128, KQLScopeToType(DB::KQLDataType::Decimal)},
    {DB::TypeIndex::Decimal256, KQLScopeToType(DB::KQLDataType::Invalid)},
    {DB::TypeIndex::Enum16, KQLScopeToType(DB::KQLDataType::Invalid)},
    {DB::TypeIndex::Enum8, KQLScopeToType(DB::KQLDataType::Invalid)},
    {DB::TypeIndex::FixedString, KQLScopeToType(DB::KQLDataType::String)},
    {DB::TypeIndex::Float32, KQLScopeToType(DB::KQLDataType::Real)},
    {DB::TypeIndex::Float64, KQLScopeToType(DB::KQLDataType::Real)},
    {DB::TypeIndex::Function, KQLScopeToType(DB::KQLDataType::Invalid)},
    {DB::TypeIndex::Int8, KQLScopeToType(DB::KQLDataType::Int)},
    {DB::TypeIndex::Int16, KQLScopeToType(DB::KQLDataType::Int)},
    {DB::TypeIndex::Int32, KQLScopeToType(DB::KQLDataType::Int)},
    {DB::TypeIndex::Int64, KQLScopeToType(DB::KQLDataType::Long)},
    {DB::TypeIndex::Int128, KQLScopeToType(DB::KQLDataType::Invalid)},
    {DB::TypeIndex::Int256, KQLScopeToType(DB::KQLDataType::Invalid)},
    {DB::TypeIndex::Interval, KQLScopeToType(DB::KQLDataType::Timespan)},
    {DB::TypeIndex::IPv4, KQLScopeToType(DB::KQLDataType::Invalid)},
    {DB::TypeIndex::IPv6, KQLScopeToType(DB::KQLDataType::Invalid)},
    {DB::TypeIndex::LowCardinality, KQLScopeToType(DB::KQLDataType::Invalid)},
    {DB::TypeIndex::Map, KQLScopeToType(DB::KQLDataType::Invalid)},
    {DB::TypeIndex::Nothing, KQLScopeToType(DB::KQLDataType::Invalid)},
    {DB::TypeIndex::Nullable, KQLScopeToType(DB::KQLDataType::Invalid)},
    {DB::TypeIndex::Object, KQLScopeToType(DB::KQLDataType::Invalid)},
    {DB::TypeIndex::Set, KQLScopeToType(DB::KQLDataType::Invalid)},
    {DB::TypeIndex::String, KQLScopeToType(DB::KQLDataType::String)},
    {DB::TypeIndex::Tuple, KQLScopeToType(DB::KQLDataType::Invalid)},
    {DB::TypeIndex::UInt8, KQLScopeToType(DB::KQLDataType::Int)},
    {DB::TypeIndex::UInt16, KQLScopeToType(DB::KQLDataType::Int)},
    {DB::TypeIndex::UInt32, KQLScopeToType(DB::KQLDataType::Int)},
    {DB::TypeIndex::UInt64, KQLScopeToType(DB::KQLDataType::Long)},
    {DB::TypeIndex::UInt128, KQLScopeToType(DB::KQLDataType::Invalid)},
    {DB::TypeIndex::UInt256, KQLScopeToType(DB::KQLDataType::Invalid)},
    {DB::TypeIndex::UUID, KQLScopeToType(DB::KQLDataType::Guid)}};
}

namespace DB
{
KQLDataType toKQLDataType(const TypeIndex type_id, const KQLScope scope)
{
    const auto it = CLICKHOUSE_TO_KQL_TYPE.find(type_id);
    if (it == CLICKHOUSE_TO_KQL_TYPE.cend())
        throw Exception(ErrorCodes::UNKNOWN_TYPE, "Unable to map {} to a KQL type", magic_enum::enum_name(type_id));

    return it->second.getType(scope);
}

std::string toString(const KQLDataType data_type)
{
    if (data_type == KQLDataType::Invalid)
        return "n/a";

    const auto data_type_name = magic_enum::enum_name(data_type);
    std::string str(data_type_name.data(), data_type_name.length());
    std::ranges::transform(str, str.begin(), [](const unsigned char c) { return static_cast<char>(std::tolower(c)); });
    return str;
}
}
