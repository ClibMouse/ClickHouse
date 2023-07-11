#pragma once

#include <Interpreters/Context_fwd.h>
#include <string_view>
#include <unordered_set>
#include <optional>
#include <Interpreters/Context.h>

namespace DB
{
class KQLContext
{
public:
    void checkForDefaultColumnName(const std::string & column_name);
    std::string nextDefaultColumnName();
    std::optional<std::shared_ptr<Context>> context;

private:
    int next_column_ordinal = 1;
    std::unordered_set<int> taken_upcoming_column_ordinals;
};
}
