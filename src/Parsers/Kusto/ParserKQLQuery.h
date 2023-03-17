#pragma once

#include "KQLContext.h"

#include <Parsers/ASTSelectQuery.h>
#include <Parsers/IParserBase.h>

namespace DB
{
using OperationsPos = std::vector<std::pair<String, IParser::Pos>>;

class ParserKQLBase : public IParserBase
{
public:
    ~ParserKQLBase() override = default;

    static String getExprFromToken(Pos & pos);
    static String getExprFromToken(const String & text, const uint32_t max_depth);
    static String getExprFromPipe(Pos & pos);
    static bool setSubQuerySource(
        ASTPtr & select_query,
        ASTPtr & source,
        const bool dest_is_subquery,
        const bool src_is_subquery,
        const String alias = "",
        const int32_t table_index = 0);
    static bool parseSQLQueryByString(ParserPtr && parser, String & query, ASTPtr & select_node, int32_t max_depth);
    bool parseByString(const String expr, ASTPtr & node, const uint32_t max_depth);
    virtual bool updatePipeLine(OperationsPos & /*operations*/, String & /*query*/) { return false; }
};

class ParserKQLQuery : public IParserBase
{
public:
    struct KQLOperatorDataFlowState
    {
        String operator_name;
        bool input_as_subquery;
        bool output_as_subquery;
        bool need_reinterpret;
        int8_t backspace_steps; // how many steps to last token of previous pipe
    };

    explicit ParserKQLQuery(KQLContext & kql_context_) : kql_context(kql_context_) { }

    static bool getOperations(Pos & pos, Expected & expected, OperationsPos & operation_pos);

protected:
    std::unique_ptr<ParserKQLBase> getOperator(std::string_view op_name);
    static bool pre_process(String & source, Pos & pos);
    const char * getName() const override { return "KQL query"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
    bool executeImpl(Pos & pos, ASTPtr & node, Expected & expected);

private:
    KQLContext & kql_context;
};

class ParserKQLSubquery : public ParserKQLBase
{
public:
    explicit ParserKQLSubquery(KQLContext & kql_context_) : kql_context(kql_context_) { }

protected:
    const char * getName() const override { return "KQL subquery"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;

private:
    KQLContext & kql_context;
};

class ParserSimpleCHSubquery : public ParserKQLBase
{
public:
    ParserSimpleCHSubquery(ASTPtr parent_select_node_ = nullptr) { parent_select_node = parent_select_node_; }

protected:
    const char * getName() const override { return "Simple ClickHouse subquery"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
    ASTPtr parent_select_node;
};

class BracketCount
{
public:
    void count(IParser::Pos & pos)
    {
        if (pos->type == TokenType::OpeningRoundBracket)
            ++round_bracket_count;
        if (pos->type == TokenType::ClosingRoundBracket)
            --round_bracket_count;
        if (pos->type == TokenType::OpeningSquareBracket)
            ++square_bracket_count;
        if (pos->type == TokenType::ClosingSquareBracket)
            --square_bracket_count;
    }
    bool isZero() const { return round_bracket_count == 0 && square_bracket_count == 0; }

private:
    int16_t round_bracket_count = 0;
    int16_t square_bracket_count = 0;
};
}
