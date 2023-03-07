#pragma once

#include "ParserKQLQuery.h"

namespace DB
{

class ParserKQLGetSchema : public ParserKQLBase
{
protected:
    const char * getName() const override { return "KQL getschema"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};

}
