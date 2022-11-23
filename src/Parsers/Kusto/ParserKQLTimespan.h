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
    static std::optional<Int64> parse(const std::string & expression);
    static bool tryParse(const std::string & expression, std::optional<Int64> & ticks);
};
}
