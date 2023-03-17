#pragma once

#include "KQLContext.h"

#include <Parsers/IParserBase.h>

namespace DB
{

class ParserKQLStatement : public IParserBase
{
protected:
    const char * getName() const override { return "KQL Statement"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};

class ParserKQLWithOutput : public IParserBase
{
public:
    explicit ParserKQLWithOutput(KQLContext & kql_context_) : kql_context(kql_context_) { }

protected:
    const char * getName() const override { return "KQL with output"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;

private:
    KQLContext & kql_context;
};

class ParserKQLWithUnionQuery : public IParserBase
{
public:
    explicit ParserKQLWithUnionQuery(KQLContext & kql_context_) : kql_context(kql_context_) { }

protected:
    const char * getName() const override { return "KQL query, possibly with UNION"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;

private:
    KQLContext & kql_context;
};

class ParserKQLTableFunction : public IParserBase
{
public:
    explicit ParserKQLTaleFunction(KQLContext & kql_context_) : kql_context(kql_context_) { }

protected:
    const char * getName() const override { return "KQL() function"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;

private:
    KQLContext & kql_context;
};

}
