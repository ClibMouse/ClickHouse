#include "KQLContext.h"

#include <format>

namespace
{
constexpr std::string_view ANONYMOUS_COLUMN_NAME = "Column";
}

namespace DB
{
void KQLContext::checkForDefaultColumnName(const std::string & column_name)
{
    if (!column_name.starts_with(ANONYMOUS_COLUMN_NAME))
        return;

    try
    {
        size_t pos;
        const auto column_ordinal = std::stoi(column_name.substr(ANONYMOUS_COLUMN_NAME.length()), &pos);
        if (column_ordinal >= next_column_ordinal && column_name.length() == pos + ANONYMOUS_COLUMN_NAME.length())
            taken_upcoming_column_ordinals.insert(column_ordinal);
    }
    catch (...)
    {
        // if conversion to integer failed, we can just carry on
    }
}

std::string KQLContext::nextDefaultColumnName()
{
    while (taken_upcoming_column_ordinals.contains(next_column_ordinal))
    {
        taken_upcoming_column_ordinals.erase(next_column_ordinal);
        ++next_column_ordinal;
    }

    const auto column_ordinal = next_column_ordinal;
    ++next_column_ordinal;

    return std::format("{}{}", ANONYMOUS_COLUMN_NAME, column_ordinal);
}
}
