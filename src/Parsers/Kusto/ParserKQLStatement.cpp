#include "KQLContext.h"
#include "Utilities.h"

#include <Parsers/IParserBase.h>
#include <Parsers/ParserSetQuery.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/Kusto/ParserKQLQuery.h>
#include <Parsers/Kusto/ParserKQLStatement.h>
#include <Parsers/Kusto/Utilities.h>
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
            auto kal_table_str = String(pos->begin, pos->end);
            auto heredoc_name_end_position = kal_table_str.find('$', 1);
            if (heredoc_name_end_position != std::string::npos)
            {
                size_t heredoc_size = heredoc_name_end_position + 1;
                std::string_view heredoc = {kal_table_str.data(), heredoc_size};

                size_t heredoc_end_position = kal_table_str.find(heredoc, heredoc_size);
                if (heredoc_end_position != std::string::npos)
                {
                    kql_statement = kal_table_str.substr(heredoc_name_end_position + 1, heredoc_end_position - heredoc_name_end_position - 1);
                }
            }
        }
        else
        {
            ++paren_count;
            auto pos_start = pos;
            while (isValidKQLPos(pos))
            {
                if (pos->type == TokenType::ClosingRoundBracket)
                    --paren_count;
                if (pos->type == TokenType::OpeningRoundBracket)
                    ++paren_count;

                if (paren_count == 0)
                    break;
                ++pos;
            }
            if (!isValidKQLPos(pos) && paren_count != 0)
                return false;

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
};
}
