#pragma once

#include <Core/Types.h>

#include <string>

namespace DB
{
enum class KQLDataType
{
    Array,
    Bool,
    DateTime,
    Decimal,
    Dictionary,
    Dynamic,
    Guid,
    Int,
    Invalid,
    Long,
    Real,
    String,
    Timespan
};

enum class KQLScope
{
    Column,
    Row
};

KQLDataType toKQLDataType(TypeIndex type_id, KQLScope scope);
std::string toString(KQLDataType data_type);
}
