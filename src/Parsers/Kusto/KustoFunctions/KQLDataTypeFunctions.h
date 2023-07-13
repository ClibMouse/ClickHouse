#pragma once

#include "IParserKQLFunction.h"

#define DATE_KQL_MIN_YEAR 1900 
#define DATE_KQL_MAX_YEAR 2261 /// Last supported year(complete) in KQL 

namespace DB
{
class DatatypeBool : public IParserKQLFunction
{
protected:
    const char * getName() const override { return "bool(),boolean()"; }
    bool convertImpl(String &out,IParser::Pos &pos) override;
};

class DatatypeDatetime : public IParserKQLFunction
{
protected:
    const char * getName() const override { return "datetime(),date()"; }
    bool convertImpl(String &out,IParser::Pos &pos) override;
};

class DatatypeDynamic : public IParserKQLFunction
{
protected:
    const char * getName() const override { return "dynamic()"; }
    bool convertImpl(String &out,IParser::Pos &pos) override;
};

class DatatypeGuid : public IParserKQLFunction
{
protected:
    const char * getName() const override { return "guid()"; }
    bool convertImpl(String &out,IParser::Pos &pos) override;
};

class DatatypeInt : public IParserKQLFunction
{
protected:
    const char * getName() const override { return "int()"; }
    bool convertImpl(String &out,IParser::Pos &pos) override;
};

class DatatypeLong : public IParserKQLFunction
{
protected:
    const char * getName() const override { return "long()"; }
    bool convertImpl(String &out,IParser::Pos &pos) override;
};

class DatatypeReal : public IParserKQLFunction
{
protected:
    const char * getName() const override { return "real(),double()"; }
    bool convertImpl(String &out,IParser::Pos &pos) override;
};

class DatatypeTimespan : public IParserKQLFunction
{
protected:
    const char * getName() const override { return "timespan(), time()"; }
    bool convertImpl(String &out,IParser::Pos &pos) override;
};

class DatatypeDecimal : public IParserKQLFunction
{
protected:
    const char * getName() const override { return "decimal()"; }
    bool convertImpl(String &out,IParser::Pos &pos) override;
};
}
