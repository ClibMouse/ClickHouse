#pragma once

#include <Parsers/IParserBase.h>
#include <Parsers/Kusto/ParserKQLQuery.h>

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

private:
    String getRenameExprFromToken(Pos & pos)
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
