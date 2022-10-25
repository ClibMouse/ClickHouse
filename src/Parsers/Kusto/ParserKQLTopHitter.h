#pragma once

#include <Parsers/IParserBase.h>
#include <Parsers/Kusto/ParserKQLQuery.h>

namespace DB
{

class ParserKQLTopHitters : public ParserKQLBase
{
protected:
    const char * getName() const override { return "KQL top-hitters"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
    bool updatePipeLine (OperationsPos & operations, String & query) override;
};

}
