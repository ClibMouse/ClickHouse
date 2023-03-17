#include "KQLContext.h"
#include "Utilities.h"

#include <Parsers/IParserBase.h>
#include <Parsers/ParserSetQuery.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/Kusto/ParserKQLQuery.h>
#include <Parsers/Kusto/ParserKQLStatement.h>
#include <Parsers/CommonParsers.h>

namespace DB
{

bool ParserKQLStatement::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    KQLContext kql_context;

    ParserKQLWithOutput query_with_output_p(kql_context);
    ParserSetQuery set_p;

    bool res = query_with_output_p.parse(pos, node, expected) || set_p.parse(pos, node, expected);

    return res;
}

bool ParserKQLWithOutput::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ParserKQLWithUnionQuery kql_p(kql_context);

    ASTPtr query;
    bool parsed = kql_p.parse(pos, query, expected);

    if (!parsed)
        return false;

    node = std::move(query);
    return true;
}

bool ParserKQLWithUnionQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    // will support union next phase
    ASTPtr kql_query;
    if (!ParserKQLQuery(kql_context).parse(pos, kql_query, expected))
        return false;

    if (kql_query->as<ASTSelectWithUnionQuery>())
    {
        node = std::move(kql_query);
        return true;
    }

    node = wrapInSelectWithUnion(kql_query);
    return true;
}

bool ParserKQLTableFunction::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ParserKQLWithUnionQuery kql_p(kql_context);
    ASTPtr select;
    ParserToken s_lparen(TokenType::OpeningRoundBracket);

    auto begin = pos;
    auto paren_count = 0;
    String kql_statement;

    if (s_lparen.ignore(pos, expected))
    {
        if (pos->type == TokenType::HereDoc)
        {
            kql_statement = String(pos->begin + 2, pos->end - 2);
        }
        else
        {
            ++paren_count;
            auto pos_start = pos;
            while (!pos->isEnd())
            {
                if (pos->type == TokenType::ClosingRoundBracket)
                    --paren_count;
                if (pos->type == TokenType::OpeningRoundBracket)
                    ++paren_count;

                if (paren_count == 0)
                    break;
                ++pos;
            }
            kql_statement = String(pos_start->begin, (--pos)->end);
        }
        ++pos;
        Tokens token_kql(kql_statement.c_str(), kql_statement.c_str() + kql_statement.size());
        IParser::Pos pos_kql(token_kql, pos.max_depth);

        if (kql_p.parse(pos_kql, select, expected))
        {
            node = select;
            ++pos;
            return true;
        }
    }
    pos = begin;
    return false;
}
}
