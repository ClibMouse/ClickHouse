#pragma once

#include "IParserKQLFunction.h"

namespace DB
{
class Abs : public IParserKQLFunction
{
protected:
    const char * getName() const override { return "abs()"; }
    bool convertImpl(String &out,IParser::Pos &pos) override;
};

class Ceiling : public IParserKQLFunction
{
protected:
    const char * getName() const override { return "ceiling()"; }
    bool convertImpl(String &out,IParser::Pos &pos) override;
};

class Exp : public IParserKQLFunction
{
protected:
    const char * getName() const override { return "exp()"; }
    bool convertImpl(String &out,IParser::Pos &pos) override;
};

class Exp2 : public IParserKQLFunction
{
protected:
    const char * getName() const override { return "exp2()"; }
    bool convertImpl(String &out,IParser::Pos &pos) override;
};

class Exp10 : public IParserKQLFunction
{
protected:
    const char * getName() const override { return "exp10()"; }
    bool convertImpl(String &out,IParser::Pos &pos) override;
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
    bool convertImpl(String &out,IParser::Pos &pos) override;
};

class Log2 : public IParserKQLFunction
{
protected:
    const char * getName() const override { return "log2"; }
    bool convertImpl(String &out,IParser::Pos &pos) override;
};

class Log10 : public IParserKQLFunction
{
protected:
    const char * getName() const override { return "log10()"; }
    bool convertImpl(String &out,IParser::Pos &pos) override;
};

class Pow : public IParserKQLFunction
{
protected:
    const char * getName() const override { return "pow()"; }
    bool convertImpl(String &out,IParser::Pos &pos) override;
};

class Sqrt : public IParserKQLFunction
{
protected:
    const char * getName() const override { return "sqrt()"; }
    bool convertImpl(String &out,IParser::Pos &pos) override;
};

}

