#pragma once

#include "IParserKQLFunction.h"

namespace DB
{
class KQLFunctionFactory
{
public:
    static std::unique_ptr<IParserKQLFunction> get(const String & kql_function);
};
}
