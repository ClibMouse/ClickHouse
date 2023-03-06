#include "KQLDataType.h"

#include <Common/Exception.h>

#include <magic_enum.hpp>

#include <algorithm>

namespace DB::ErrorCodes
{
extern const int UNKNOWN_TYPE;
}

namespace
{
const std::unordered_map<DB::TypeIndex, DB::KQLDataType> CLICKHOUSE_TO_KQL_TYPE{
    {DB::TypeIndex::AggregateFunction, DB::KQLDataType::Invalid},
    {DB::TypeIndex::Array, DB::KQLDataType::Dynamic},
    {DB::TypeIndex::Date, DB::KQLDataType::DateTime},
    {DB::TypeIndex::Date32, DB::KQLDataType::DateTime},
    {DB::TypeIndex::DateTime, DB::KQLDataType::DateTime},
    {DB::TypeIndex::DateTime64, DB::KQLDataType::DateTime},
    {DB::TypeIndex::Decimal32, DB::KQLDataType::Decimal},
    {DB::TypeIndex::Decimal64, DB::KQLDataType::Decimal},
    {DB::TypeIndex::Decimal128, DB::KQLDataType::Decimal},
    {DB::TypeIndex::Decimal256, DB::KQLDataType::Decimal},
    {DB::TypeIndex::Enum16, DB::KQLDataType::Invalid},
    {DB::TypeIndex::Enum8, DB::KQLDataType::Invalid},
    {DB::TypeIndex::FixedString, DB::KQLDataType::String},
    {DB::TypeIndex::Float32, DB::KQLDataType::Real},
    {DB::TypeIndex::Float64, DB::KQLDataType::Real},
    {DB::TypeIndex::Function, DB::KQLDataType::Invalid},
    {DB::TypeIndex::Int8, DB::KQLDataType::Int},
    {DB::TypeIndex::Int16, DB::KQLDataType::Int},
    {DB::TypeIndex::Int32, DB::KQLDataType::Int},
    {DB::TypeIndex::Int64, DB::KQLDataType::Long},
    {DB::TypeIndex::Int128, DB::KQLDataType::Long},
    {DB::TypeIndex::Int256, DB::KQLDataType::Long},
    {DB::TypeIndex::Interval, DB::KQLDataType::Timespan},
    {DB::TypeIndex::IPv4, DB::KQLDataType::Invalid},
    {DB::TypeIndex::IPv6, DB::KQLDataType::Invalid},
    {DB::TypeIndex::LowCardinality, DB::KQLDataType::Invalid},
    {DB::TypeIndex::Map, DB::KQLDataType::Invalid},
    {DB::TypeIndex::Nothing, DB::KQLDataType::Invalid},
    {DB::TypeIndex::Nullable, DB::KQLDataType::Invalid},
    {DB::TypeIndex::Object, DB::KQLDataType::Invalid},
    {DB::TypeIndex::Set, DB::KQLDataType::Dynamic},
    {DB::TypeIndex::String, DB::KQLDataType::String},
    {DB::TypeIndex::Tuple, DB::KQLDataType::Invalid},
    {DB::TypeIndex::UInt8, DB::KQLDataType::Int},
    {DB::TypeIndex::UInt16, DB::KQLDataType::Int},
    {DB::TypeIndex::UInt32, DB::KQLDataType::Int},
    {DB::TypeIndex::UInt64, DB::KQLDataType::Long},
    {DB::TypeIndex::UInt128, DB::KQLDataType::Long},
    {DB::TypeIndex::UInt256, DB::KQLDataType::Long},
    {DB::TypeIndex::UUID, DB::KQLDataType::Guid}};
}

namespace DB
{
KQLDataType toKQLDataType(const TypeIndex type_id)
{
    const auto it = CLICKHOUSE_TO_KQL_TYPE.find(type_id);
    if (it == CLICKHOUSE_TO_KQL_TYPE.cend())
        throw Exception(ErrorCodes::UNKNOWN_TYPE, "Unable to map {} to a KQL type", magic_enum::enum_name(type_id));

    return it->second;
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
