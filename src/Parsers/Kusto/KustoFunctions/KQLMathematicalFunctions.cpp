#include "KQLMathematicalFunctions.h"

#include <format>

namespace DB
{

bool Abs::convertImpl(String & out, IParser::Pos & pos)
{
    return directMapping(out, pos, "abs");
}

bool Acos::convertImpl(String & out, IParser::Pos & pos)
{
    return directMapping(out, pos, "acos");
}

bool Asin::convertImpl(String & out, IParser::Pos & pos)
{
    return directMapping(out, pos, "asin");
}

bool Atan::convertImpl(String & out, IParser::Pos & pos)
{
    return directMapping(out, pos, "atan");
}

bool Atan2::convertImpl(String & out, IParser::Pos & pos)
{
    return directMapping(out, pos, "atan2");
}

bool Ceiling::convertImpl(String & out, IParser::Pos & pos)
{
    return directMapping(out, pos, "ceil");
}

bool Cos::convertImpl(String & out, IParser::Pos & pos)
{
    return directMapping(out, pos, "cos");
}

bool Cot::convertImpl(String & out, IParser::Pos & pos)
{
    const auto fn_name = getKQLFunctionName(pos);

    if (fn_name.empty())
        return false;
    out = "1/tan(" + getArgument(fn_name, pos) + ")";

    return true;
}

bool Degrees::convertImpl(String & out, IParser::Pos & pos)
{
    return directMapping(out, pos, "degrees");
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

bool Gamma::convertImpl(String & out, IParser::Pos & pos)
{
    return directMapping(out, pos, "tgamma");
}

bool IsFinite::convertImpl(String & out, IParser::Pos & pos)
{
    return directMapping(out, pos, "isFinite");
}

bool IsInfinite::convertImpl(String & out, IParser::Pos & pos)
{
    return directMapping(out, pos, "isInfinite");
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

bool LogGamma::convertImpl(String & out, IParser::Pos & pos)
{
    return directMapping(out, pos, "lgamma");
}

bool MaxOf::convertImpl(String & out, IParser::Pos & pos)
{
    const auto fn_name = getKQLFunctionName(pos);

    if (fn_name.empty())
        return false;

    out.append("arrayReduce('max', [");
    const auto arguments = getArguments(fn_name, pos, ArgumentState::Parsed, {2, 64});

    for (size_t i = 0; i < arguments.size(); i++)
    {
        out.append(arguments[i]);
        if (i < arguments.size() - 1)
            out.append(", ");
    }
    out.append("])");
    return true;
}

bool MinOf::convertImpl(String & out, IParser::Pos & pos)
{
    const auto fn_name = getKQLFunctionName(pos);

    if (fn_name.empty())
        return false;

    out.append("arrayReduce('min', [");
    const auto arguments = getArguments(fn_name, pos, ArgumentState::Parsed, {2, 64});

    for (size_t i = 0; i < arguments.size(); i++)
    {
        out.append(arguments[i]);
        if (i < arguments.size() - 1)
            out.append(", ");
    }
    out.append("])");
    return true;
}

bool Pi::convertImpl(String & out, IParser::Pos & pos)
{
    return directMapping(out, pos, "pi");
}

bool Pow::convertImpl(String & out, IParser::Pos & pos)
{
    return directMapping(out, pos, "pow");
}

bool Radians::convertImpl(String & out, IParser::Pos & pos)
{
    return directMapping(out, pos, "radians");
}

bool Rand::convertImpl(String & out, IParser::Pos & pos)
{
    const auto fn_name = getKQLFunctionName(pos);

    if (fn_name.empty())
        return false;
    const auto arg = getOptionalArgument(fn_name, pos).value_or("0");
    out.append("if(" + arg + " < 2, randCanonical(), moduloOrZero(rand()," + arg + "))");
    return true;
}

bool Round::convertImpl(String & out, IParser::Pos & pos)
{
    return directMapping(out, pos, "round");
}

bool Sign::convertImpl(String & out, IParser::Pos & pos)
{
    return directMapping(out, pos, "sign");
}

bool Sin::convertImpl(String & out, IParser::Pos & pos)
{
    return directMapping(out, pos, "sin");
}

bool Sqrt::convertImpl(String & out, IParser::Pos & pos)
{
    return directMapping(out, pos, "sqrt");
}

bool Tan::convertImpl(String & out, IParser::Pos & pos)
{
    return directMapping(out, pos, "tan");
}

}
