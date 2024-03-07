#pragma once

#include <Parsers/IParser.h>

namespace DB
{
class KQLOperators
{
public:
    static bool convert(std::vector<String> & tokens, IParser::Pos & pos);
};
}
