#include <Parsers/ASTLiteral.h>
#include <Parsers/IParserBase.h>
#include <Parsers/ParserTablesInSelectQuery.h>
#include <Parsers/Kusto/ParserKQLQuery.h>
#include <Parsers/Kusto/ParserKQLTable.h>

#include <unordered_set>

namespace
{
const std::unordered_set<String> sql_keywords{"SELECT",   "INSERT", "CREATE",   "ALTER",    "SYSTEM", "SHOW",   "GRANT",  "REVOKE",
                                              "ATTACH",   "CHECK",  "DESCRIBE", "DESC",     "DETACH", "DROP",   "EXISTS", "KILL",
                                              "OPTIMIZE", "RENAME", "SET",      "TRUNCATE", "USE",    "EXPLAIN"};
}

namespace DB
{

bool ParserKQLTable ::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ASTPtr tables;
    String table_name(pos->begin, pos->end);
    String table_name_upcase(table_name);

    std::ranges::transform(table_name_upcase, table_name_upcase.begin(), toupper);
    if (sql_keywords.contains(table_name_upcase))
        return false;

    if (!ParserTablesInSelectQuery().parse(pos, tables, expected))
        return false;

    node->as<ASTSelectQuery>()->setExpression(ASTSelectQuery::Expression::TABLES, std::move(tables));

    return true;
}

}
