#include "KQLMathematicalFunctions.h"

#include <format>

namespace DB
{

bool Abs::convertImpl(String & out, IParser::Pos & pos)
{
    return directMapping(out, pos, "abs");
}

bool Ceiling::convertImpl(String & out, IParser::Pos & pos)
{
    return directMapping(out, pos, "ceil");
}

bool Exp::convertImpl(String & out, IParser::Pos & pos)
{
    return directMapping(out, pos, "exp");
}

bool Exp2::convertImpl(String & out, IParser::Pos & pos)
{
    return directMapping(out, pos, "exp2");
}

bool Exp10::convertImpl(String & out, IParser::Pos & pos)
{
    return directMapping(out, pos, "exp10");
}

bool IsNan::convertImpl(String & out, IParser::Pos & pos)
{
    const auto function_name = getKQLFunctionName(pos);
    if (function_name.empty())
        return false;

    const auto argument = getArgument(function_name, pos);
    out = std::format(
        "if(toTypeName({0}) in ['Float64', 'Nullable(Float64)'], isNaN({0}), throwIf(true, 'Expected argument of data type real'))",
        argument);

    return true;
}

bool Log::convertImpl(String & out, IParser::Pos & pos)
{
    return directMapping(out, pos, "log");
}

bool Log2::convertImpl(String & out, IParser::Pos & pos)
{
    return directMapping(out, pos, "log2");
}

bool Log10::convertImpl(String & out, IParser::Pos & pos)
{
    return directMapping(out, pos, "log10");
}

bool Pow::convertImpl(String & out, IParser::Pos & pos)
{
    return directMapping(out, pos, "pow");
}

bool Sqrt::convertImpl(String & out, IParser::Pos & pos)
{
    return directMapping(out, pos, "sqrt");
}

}
