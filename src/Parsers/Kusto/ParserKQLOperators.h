#pragma once

#include <Parsers/IParser.h>

namespace DB
{
class KQLOperators
{
public:
    bool static convert(std::vector<String> & tokens, IParser::Pos & pos);
};
}
