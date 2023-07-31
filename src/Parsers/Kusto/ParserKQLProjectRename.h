#pragma once

#include "Utilities.h"
#include <Parsers/IParserBase.h>
#include <Parsers/Kusto/ParserKQLQuery.h>
#include <iostream>
#include <Parsers/ASTTablesInSelectQuery.h>

#include <Parsers/ASTAsterisk.h>
#include <Parsers/ASTColumnsTransformers.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ExpressionListParsers.h>
#include <Interpreters/InterpreterSelectWithUnionQuery.h>
#include <Interpreters/Context.h>
#include <Interpreters/Context_fwd.h>
#include <Common/CurrentThread.h>
#include <format>
#include <optional>
#include <ranges>
#include <algorithm>

namespace DB
{
namespace ErrorCodes
{
extern const int SYNTAX_ERROR;
}

class ParserKQLProjectRename : public ParserKQLBase
{
public:
    explicit ParserKQLProjectRename(KQLContext & kql_context_) : kql_context(kql_context_) { }

protected:
    const char * getName() const override { return "KQL project-rename"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;

public:
    bool checkDuplicateAlias(const ASTPtr & node, const std::string & alias)
    {
        if (kql_context.context)
        {
            if (auto * select_query = node->as<ASTSelectQuery>(); !select_query->select())
                setSelectAll(*select_query);
            const auto sample_block = InterpreterSelectWithUnionQuery::getSampleBlock(wrapInSelectWithUnion(node), kql_context.context);
            const auto & names_and_types = sample_block.getNamesAndTypes();
            
            if (std::ranges::find_if(names_and_types.begin(), names_and_types.end(), [&alias](DB::NameAndTypePair x) { return x.name == alias; }) != names_and_types.end())
                return true;
            /*
            for (int i = 0; i < std::ssize(names_and_types); ++i)
            {
                if (names_and_types[i].name == alias)
                    return true;
            }
            */
        }
        
        return false;
    }

    String getRenameExprFromToken(Pos & pos, ASTPtr & node)
    {
        String rename_expr;
        std::vector<String> seen_columns;
        auto last_pos = pos;
        auto rename_assignment = true;
        size_t bracket_count = 0;

        while (!pos->isEnd() && pos->type != TokenType::PipeMark && pos->type != TokenType::Semicolon)
        {
            if (pos->type == TokenType::OpeningRoundBracket)
                ++bracket_count;
            else if (pos->type == TokenType::ClosingRoundBracket)
                --bracket_count;
            else if (!bracket_count && pos->type == TokenType::Equals && String(pos->begin, pos->end) == "=")
            {
                --pos;
                if (pos->type == TokenType::BareWord)
                {
                    const auto alias = String(pos->begin, pos->end);
                    if (checkDuplicateAlias(node, alias))
                    {
                        throw Exception(ErrorCodes::SYNTAX_ERROR, "Syntax error: duplicate column name '{}'", alias);
                    }
                }
                ++pos;
                ++pos;
                if (pos->type == TokenType::BareWord)
                {
                    auto column = String(pos->begin, pos->end);

                    if (std::ranges::find(seen_columns.begin(), seen_columns.end(), column) == seen_columns.end())
                    {
                        rename_assignment = true;
                        seen_columns.push_back(column);
                    }
                    else
                        throw Exception(ErrorCodes::SYNTAX_ERROR, "Syntax error: column '{}' renamed multiple times", column);
                }
                --pos;
            }
            else if (!bracket_count && pos->type == TokenType::Comma)
            {
                if (rename_assignment)
                {
                    rename_expr += (last_pos < pos) ? String(last_pos->begin, pos->end - 1) : "";
                    rename_assignment = false;
                }
                last_pos = pos;
            }
            ++pos;
        }
        if (rename_assignment)
            rename_expr += (last_pos < pos) ? String(last_pos->begin, pos->end) : "";

        Tokens rename_tokens(rename_expr.c_str(), rename_expr.c_str() + rename_expr.size());
        IParser::Pos rename_pos(rename_tokens, pos.max_depth);
        return getExprFromToken(rename_pos);
    }
    KQLContext & kql_context;
};

}
