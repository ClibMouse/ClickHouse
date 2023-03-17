#pragma once

#include <Parsers/IParserBase.h>
#include <Parsers/Kusto/ParserKQLQuery.h>

namespace DB
{

class ParserKQLExtend : public ParserKQLBase
{
public:
    explicit ParserKQLExtend(KQLContext & kql_context_) : kql_context(kql_context_) { }

protected:
    const char * getName() const override { return "KQL extend"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;

private:
    KQLContext & kql_context;
};

}
