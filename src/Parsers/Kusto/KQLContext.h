#pragma once

#include <string_view>
#include <unordered_set>

namespace DB
{
class KQLContext
{
public:
    void checkForDefaultColumnName(const std::string & column_name);
    std::string nextDefaultColumnName();

private:
    int next_column_ordinal = 1;
    std::unordered_set<int> taken_upcoming_column_ordinals;
};
}
