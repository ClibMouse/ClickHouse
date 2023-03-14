#pragma once

#include "IParserKQLFunction.h"

namespace DB
{
class Hash : public IParserKQLFunction
{
protected:
    const char * getName() const override { return "hash()"; }
    bool convertImpl(String & out, IParser::Pos & pos) override;
};

class HashSha256 : public IParserKQLFunction
{
protected:
    const char * getName() const override { return "hash_sha256()"; }
    bool convertImpl(String & out, IParser::Pos & pos) override;
};

}
