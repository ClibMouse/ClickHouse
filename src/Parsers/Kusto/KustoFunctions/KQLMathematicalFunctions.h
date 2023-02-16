#pragma once

#include "IParserKQLFunction.h"

namespace DB
{
class Abs : public IParserKQLFunction
{
protected:
    const char * getName() const override { return "abs()"; }
    bool convertImpl(String & out, IParser::Pos & pos) override;
};

class Acos : public IParserKQLFunction
{
protected:
    const char * getName() const override { return "acos()"; }
    bool convertImpl(String & out, IParser::Pos & pos) override;
};

class Asin : public IParserKQLFunction
{
protected:
    const char * getName() const override { return "asin()"; }
    bool convertImpl(String & out, IParser::Pos & pos) override;
};

class Atan : public IParserKQLFunction
{
protected:
    const char * getName() const override { return "atan()"; }
    bool convertImpl(String & out, IParser::Pos & pos) override;
};

class Atan2 : public IParserKQLFunction
{
protected:
    const char * getName() const override { return "atan2()"; }
    bool convertImpl(String & out, IParser::Pos & pos) override;
};

class Ceiling : public IParserKQLFunction
{
protected:
    const char * getName() const override { return "ceiling()"; }
    bool convertImpl(String & out, IParser::Pos & pos) override;
};

class Cos : public IParserKQLFunction
{
protected:
    const char * getName() const override { return "cos()"; }
    bool convertImpl(String & out, IParser::Pos & pos) override;
};

class Cot : public IParserKQLFunction
{
protected:
    const char * getName() const override { return "cot()"; }
    bool convertImpl(String & out, IParser::Pos & pos) override;
};

class Degrees : public IParserKQLFunction
{
protected:
    const char * getName() const override { return "degrees()"; }
    bool convertImpl(String & out, IParser::Pos & pos) override;
};

class Exp : public IParserKQLFunction
{
protected:
    const char * getName() const override { return "exp()"; }
    bool convertImpl(String & out, IParser::Pos & pos) override;
};

class Exp2 : public IParserKQLFunction
{
protected:
    const char * getName() const override { return "exp2()"; }
    bool convertImpl(String & out, IParser::Pos & pos) override;
};

class Exp10 : public IParserKQLFunction
{
protected:
    const char * getName() const override { return "exp10()"; }
    bool convertImpl(String & out, IParser::Pos & pos) override;
};

class Gamma : public IParserKQLFunction
{
protected:
    const char * getName() const override { return "gamma()"; }
    bool convertImpl(String & out, IParser::Pos & pos) override;
};

class IsFinite : public IParserKQLFunction
{
protected:
    const char * getName() const override { return "isfinite()"; }
    bool convertImpl(String & out, IParser::Pos & pos) override;
};

class IsInfinite : public IParserKQLFunction
{
protected:
    const char * getName() const override { return "isinf()"; }
    bool convertImpl(String & out, IParser::Pos & pos) override;
};

class IsNan : public IParserKQLFunction
{
protected:
    const char * getName() const override { return "isnan()"; }
    bool convertImpl(String & out, IParser::Pos & pos) override;
};

class Log : public IParserKQLFunction
{
protected:
    const char * getName() const override { return "log()"; }
    bool convertImpl(String & out, IParser::Pos & pos) override;
};

class Log2 : public IParserKQLFunction
{
protected:
    const char * getName() const override { return "log2()"; }
    bool convertImpl(String & out, IParser::Pos & pos) override;
};

class Log10 : public IParserKQLFunction
{
protected:
    const char * getName() const override { return "log10()"; }
    bool convertImpl(String & out, IParser::Pos & pos) override;
};

class LogGamma : public IParserKQLFunction
{
protected:
    const char * getName() const override { return "loggamma()"; }
    bool convertImpl(String & out, IParser::Pos & pos) override;
};

class MaxOf : public IParserKQLFunction
{
protected:
    const char * getName() const override { return "max_of()"; }
    bool convertImpl(String & out, IParser::Pos & pos) override;
};

class MinOf : public IParserKQLFunction
{
protected:
    const char * getName() const override { return "min_of()"; }
    bool convertImpl(String & out, IParser::Pos & pos) override;
};

class Pi : public IParserKQLFunction
{
protected:
    const char * getName() const override { return "pi()"; }
    bool convertImpl(String & out, IParser::Pos & pos) override;
};

class Pow : public IParserKQLFunction
{
protected:
    const char * getName() const override { return "pow()"; }
    bool convertImpl(String & out, IParser::Pos & pos) override;
};

class Radians : public IParserKQLFunction
{
protected:
    const char * getName() const override { return "radians()"; }
    bool convertImpl(String & out, IParser::Pos & pos) override;
};

class Rand : public IParserKQLFunction
{
protected:
    const char * getName() const override { return "rand()"; }
    bool convertImpl(String & out, IParser::Pos & pos) override;
};

class Round : public IParserKQLFunction
{
protected:
    const char * getName() const override { return "round()"; }
    bool convertImpl(String & out, IParser::Pos & pos) override;
};

class Sign : public IParserKQLFunction
{
protected:
    const char * getName() const override { return "sign()"; }
    bool convertImpl(String & out, IParser::Pos & pos) override;
};

class Sin : public IParserKQLFunction
{
protected:
    const char * getName() const override { return "sin()"; }
    bool convertImpl(String & out, IParser::Pos & pos) override;
};

class Sqrt : public IParserKQLFunction
{
protected:
    const char * getName() const override { return "sqrt()"; }
    bool convertImpl(String & out, IParser::Pos & pos) override;
};

class Tan : public IParserKQLFunction
{
protected:
    const char * getName() const override { return "tan()"; }
    bool convertImpl(String & out, IParser::Pos & pos) override;
};

}
