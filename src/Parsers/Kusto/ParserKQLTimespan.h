#pragma once

#include <base/types.h>

#include <optional>
#include <string>

namespace DB
{
std::string kqlTicksToInterval(std::optional<Int64> ticks);

class ParserKQLTimespan
{
public:
    static std::optional<Int64> parse(std::string_view expression);
    static bool tryParse(std::string_view expression, std::optional<Int64> & ticks);
};
}
