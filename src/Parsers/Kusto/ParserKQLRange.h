#pragma once

#include <Parsers/IParserBase.h>
#include <Parsers/Kusto/ParserKQLQuery.h>

namespace DB
{

class ParserKQLRange : public ParserKQLBase
{
protected:
    const char * getName() const override { return "KQL range"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};

}
