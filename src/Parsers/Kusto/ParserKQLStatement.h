#pragma once

#include "KQLContext.h"

#include <Parsers/IParserBase.h>

namespace DB
{

class ParserKQLStatement : public IParserBase
{
protected:
    const char * end;
    bool allow_settings_after_format_in_insert;
    const char * getName() const override { return "KQL Statement"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;

public:
    explicit ParserKQLStatement(const char * end_, bool allow_settings_after_format_in_insert_ = false)
        : end(end_), allow_settings_after_format_in_insert(allow_settings_after_format_in_insert_)
    {
    }
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
