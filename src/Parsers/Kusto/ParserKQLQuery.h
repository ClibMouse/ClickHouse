#pragma once

#include <Parsers/IParserBase.h>
#include <Parsers/ASTSelectQuery.h>

namespace DB
{
using OperationsPos = std::vector<std::pair<String, IParser::Pos>>;

class ParserKQLBase : public IParserBase
{
public:
    static String getExprFromToken(Pos & pos);
    static String getExprFromToken(const String & text, const uint32_t max_depth);
    static String getExprFromPipe(Pos & pos);
    static bool setSubQuerySource(ASTPtr & select_query, ASTPtr & source, bool dest_is_subquery, bool src_is_subquery);
    static bool parseSQLQueryByString(ParserPtr && parser, String & query, ASTPtr & select_node, int32_t max_depth);
    bool parseByString(const String expr, ASTPtr & node, const uint32_t max_depth);
    virtual bool updatePipeLine (OperationsPos & /*operations*/, String & /*query*/) {return false;}
};

class ParserKQLQuery : public IParserBase
{
public:
    struct KQLOperatorDataFlowState
    {
        String operator_name;
        bool need_input;
        bool gen_output;
        bool need_reinterpret;
        int8_t backspace_steps; // how many steps to last token of previous pipe
    };
    static bool getOperations(Pos & pos, Expected & expected, OperationsPos & operation_pos);
protected:
    static std::unique_ptr<ParserKQLBase> getOperator(String &op_name);
    const char * getName() const override { return "KQL query"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};

class ParserKQLSubquery : public ParserKQLBase
{
protected:
    const char * getName() const override { return "KQL subquery"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};

class ParserSimpleCHSubquery : public ParserKQLBase
{
public:
    ParserSimpleCHSubquery(ASTPtr parent_select_node_ = nullptr) {parent_select_node = parent_select_node_;}
protected:
    const char * getName() const override { return "Simple ClickHouse subquery"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
    ASTPtr parent_select_node;
};
}
