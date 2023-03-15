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
    Guid,
    Int,
    Invalid,
    Long,
    Real,
    String,
    Timespan
};

KQLDataType toKQLDataType(TypeIndex type_id);
std::string toString(KQLDataType data_type);
}
