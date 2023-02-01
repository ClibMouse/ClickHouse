#pragma once

#include <Parsers/IParserBase.h>
#include <Parsers/Kusto/ParserKQLQuery.h>

namespace DB
{

class ParserKQLTopNested : public ParserKQLBase
{

protected:
    struct TopNestedClause
    {
        String topn;
        String expr_alias;
        String expr;
        String others_name;
        String others;
        String agg_alias;
        String agg_function;
        String agg_expr;
        String agg_column;
        String order;
    };
    using TopNestedClauses = std::vector<TopNestedClause>;
    const char * getName() const override { return "KQL top-nested"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;

    static bool parseSingleTopNestedClause(Pos & begin_pos, Pos & last_pos, TopNestedClause & top_nested_clause, const int layer);
    bool parseTopNestedClause(Pos & pos, TopNestedClauses & top_nested_clauses);
    String calculateTopNestedWithOthers(const TopNestedClauses & top_nested_clauses, const uint32_t max_depth);
    static String calculateSingleTopNestedWithOthers(const TopNestedClauses & top_nested_clauses, size_t layer, bool has_others, const uint32_t max_depth);
};

}
