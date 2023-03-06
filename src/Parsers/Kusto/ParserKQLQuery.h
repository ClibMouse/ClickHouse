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
    static bool setSubQuerySource(
        const ASTPtr & select_query,
        const ASTPtr & source,
        const bool dest_is_subquery,
        const bool src_is_subquery,
        const String & alias = "",
        const int32_t table_index = 0);
    static bool parseSQLQueryByString(ParserPtr && parser, String & query, ASTPtr & select_node, int32_t max_depth);
    bool parseByString(const String & expr, ASTPtr & node, const uint32_t max_depth);
    virtual bool updatePipeLine([[maybe_unused]] Pos pos, [[maybe_unused]] String & query) { return false; }
};

class ParserKQLQuery : public IParserBase
{
public:
    static bool getOperations(Pos & pos, Expected & expected, OperationsPos & operation_pos);

protected:
    static std::unique_ptr<ParserKQLBase> getOperator(std::string_view op_name);
    static bool preProcess(String & source, Pos & pos);
    const char * getName() const override { return "KQL query"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
    static bool executeImpl(Pos & pos, ASTPtr & node, Expected & expected);
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
    explicit ParserSimpleCHSubquery(ASTPtr parent_select_node_ = nullptr) : parent_select_node(parent_select_node_) { }

protected:
    const char * getName() const override { return "Simple ClickHouse subquery"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
    ASTPtr parent_select_node;
};
}
