#pragma once

#include "IParserKQLFunction.h"

namespace DB
{
class Bin : public IParserKQLFunction
{
protected:
    const char * getName() const override { return "bin()"; }
    bool convertImpl(String & out, IParser::Pos & pos) override;
};

class BinAt : public IParserKQLFunction
{
protected:
    const char * getName() const override { return "bin_at()"; }
    bool convertImpl(String & out, IParser::Pos & pos) override;
};

class Case : public IParserKQLFunction
{
protected:
    const char * getName() const override { return "case()"; }
    bool convertImpl(String &out,IParser::Pos &pos) override;
};

class Iff : public IParserKQLFunction
{
protected:
    const char * getName() const override { return "iff()"; }
    bool convertImpl(String &out,IParser::Pos &pos) override;
};
class Iif : public IParserKQLFunction
{
protected:
    const char * getName() const override { return "iif()"; }
    bool convertImpl(String &out,IParser::Pos &pos) override;
};

class Lookup : public IParserKQLFunction
{
protected:
    const char * getName() const override { return "lookup()"; }
    bool convertImpl(String &out,IParser::Pos &pos) override;
};

}
