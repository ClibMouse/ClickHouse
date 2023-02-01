#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTSubquery.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ExpressionListParsers.h>
#include <Parsers/IParserBase.h>
#include <Parsers/Kusto/ParserKQLJoin.h>
#include <Parsers/Kusto/ParserKQLQuery.h>
#include <Parsers/ParserSelectQuery.h>
#include <Parsers/ParserTablesInSelectQuery.h>
#include <Common/StringUtils/StringUtils.h>

#include <format>

namespace DB
{

namespace ErrorCodes
{
    extern const int SYNTAX_ERROR;
}

bool ParserKQLJoin ::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ASTPtr sub_query_node;
    String str_right_table;
    String str_attributes;
    std::vector<String> attribute_list;
    std::vector<String> left_columns;
    const String default_join = "UNINQUE INNER JOIN";
    String join_kind = default_join;
    String kql_join_kind = "innerunique";

    ParserKeyword s_kind("kind");
    ParserToken equals(TokenType::Equals);
    ParserToken open_bracket(TokenType::OpeningRoundBracket);
    ParserIdentifier id_right_table;

    size_t paren_count = 0;
    ASTPtr ast_right_table;

    std::unordered_map<String, String> join_type
        = {{"innerunique", default_join},
           {"inner", "INNER JOIN"},
           {"leftouter", "LEFT OUTER JOIN"},
           {"rightouter", "RIGHT OUTER JOIN"},
           {"fullouter", "FULL OUTER JOIN"},

           {"leftanti", "LEFT ANTI JOIN"},
           {"anti", "LEFT ANTI JOIN"},
           {"leftantisemi", "LEFT ANTI JOIN"},

           {"rightanti", "RIGHT ANTI JOIN"},
           {"rightantisemi", "RIGHT ANTI JOIN"},

           {"leftsemi", "LEFT SEMI JOIN"},
           {"rightsemi", "RIGHT SEMI JOIN"}};

    if (s_kind.ignore(pos))
    {
        if (!equals.ignore(pos))
            throw Exception("Invalid kind for join operator", ErrorCodes::SYNTAX_ERROR);

        String join_word(pos->begin, pos->end);
        if (join_type.find(join_word) == join_type.end())
            throw Exception("Invalid value of kind for join operator", ErrorCodes::SYNTAX_ERROR);

        join_kind = join_type[join_word];
        kql_join_kind = join_word;
        ++pos;
    }

    Pos right_table_start_pos = pos;
    Pos keyword_on_pos = pos;

    bool has_bracket = open_bracket.ignore(pos);

    if (!has_bracket)
    {
        if (!id_right_table.parse(pos, ast_right_table, expected))
            return false;
    }
    else
        paren_count = 1;

    Pos attributes_start_pos = pos;
    bool attributes_on_column = false;

    auto parse_attribute = [&](Pos & start_pos, Pos & end_pos)
    {
        while (start_pos < end_pos && start_pos->type == TokenType::OpeningRoundBracket)
            ++start_pos;
        while (start_pos < end_pos && end_pos->type == TokenType::ClosingRoundBracket)
            --end_pos;

        if (start_pos == end_pos)
        {
            if (start_pos->type != TokenType::BareWord)
                return false;
            attribute_list.push_back(String(start_pos->begin, end_pos->end));
            left_columns.push_back(String(start_pos->begin, end_pos->end));
        }
        else
        {
            String left_column, right_column;
            auto get_coulmn = [&]()
            {
                String left_alias = "left_.", right_alias = "right_.";
                String left_alias2 = "$left.", right_alias2 = "$right.";

                auto attribute_str = String(start_pos->begin, end_pos->end);

                if (attribute_str.substr(0, left_alias.length()) != left_alias
                    && attribute_str.substr(0, left_alias2.length()) != left_alias2)
                    return false;

                auto r_begin = attribute_str.find("==");
                if (r_begin == std::string::npos)
                    return false;
                if (attribute_str.substr(0, left_alias.length()) == left_alias)
                    left_column = attribute_str.substr(left_alias.length(), r_begin - left_alias.length());
                else
                    left_column = attribute_str.substr(left_alias2.length(), r_begin - left_alias2.length());

                r_begin += 2;
                while (r_begin < attribute_str.length() && attribute_str[r_begin] <= 0x20)
                    ++r_begin;

                if (attribute_str.substr(r_begin, right_alias.length()) != right_alias
                    && attribute_str.substr(r_begin, right_alias2.length()) != right_alias2)
                    return false;

                right_column = attribute_str.substr(r_begin + right_alias.length());
                return true;
            };

            if (!get_coulmn())
                return false;
            trim(left_column);
            trim(right_column);
            left_columns.push_back(left_column);

            if (left_column != right_column)
            {
                attributes_on_column = true;
                attribute_list.push_back(String(start_pos->begin, end_pos->end));
            }
            else
                attribute_list.push_back(left_column);
        }

        return true;
    };

    auto update_attributes = [&]
    {
        auto temp_pos = pos;
        --temp_pos;

        if (temp_pos < attributes_start_pos || !parse_attribute(attributes_start_pos, temp_pos))
            throw Exception("Attributes error for join or lookup operator", ErrorCodes::SYNTAX_ERROR);
        attributes_start_pos = pos;
        ++attributes_start_pos;
    };

    while (!pos->isEnd() && pos->type != TokenType::Semicolon)
    {
        if (pos->type == TokenType::OpeningRoundBracket)
            ++paren_count;
        if (pos->type == TokenType::ClosingRoundBracket)
            --paren_count;
        if (pos->type == TokenType::PipeMark && paren_count == 0)
            break;

        if (String(pos->begin, pos->end) == "on" && paren_count == 0)
        {
            if (keyword_on_pos == right_table_start_pos)
            {
                keyword_on_pos = pos;
                attributes_start_pos = pos;
                ++attributes_start_pos;
            }
        }

        if (pos->type == TokenType::Comma && right_table_start_pos < keyword_on_pos && paren_count == 0)
        {
            update_attributes();
        }
        ++pos;
    }

    update_attributes();

    if (keyword_on_pos <= right_table_start_pos)
        throw Exception("Missing right table or 'on' for join or lookup operator", ErrorCodes::SYNTAX_ERROR);

    --keyword_on_pos;
    if (right_table_start_pos == keyword_on_pos)
        str_right_table = String(right_table_start_pos->begin, keyword_on_pos->end);
    else
        str_right_table = std::format("kql{}", String(right_table_start_pos->begin, keyword_on_pos->end));

    ++keyword_on_pos;
    ++keyword_on_pos;
    --pos;
    if (pos < keyword_on_pos)
        throw Exception("Missing attributes for join or lookup operator", ErrorCodes::SYNTAX_ERROR);

    String query_join;
    if (join_kind == default_join)
    {
        join_kind = "INNER JOIN";
        String distinct_column;
        for (auto const & col : left_columns)
            distinct_column = distinct_column.empty() ? col : distinct_column + "," + col;

        String distinct_query = std::format("(SELECT DISTINCT ON ({}) * FROM dum_tbl)", distinct_column);
        if (!parseSQLQueryByString(std::make_unique<ParserTablesInSelectQuery>(), distinct_query, sub_query_node, pos.max_depth))
            return false;
        if (!setSubQuerySource(sub_query_node, node, true, false))
            return false;
        node = std::move(sub_query_node);
    }

    if (attributes_on_column)
    {
        auto replace = [&](std::string & str, const std::string & from, const std::string & to)
        {
            size_t start_pos = str.find(from);
            if (start_pos != std::string::npos)
                str.replace(start_pos, from.length(), to);
        };

        for (auto str : attribute_list)
        {
            if (str.substr(0, 6) != "left_." && str.substr(0, 6) != "$left.")
                str = std::format("left_.{0} == right_.{0}", str);
            else if (str.substr(0, 6) == "$left.")
            {
                replace(str, "$left.", "left_.");
                replace(str, "$right.", "right_.");
            }

            str_attributes = str_attributes.empty() ? str : str_attributes + " and " + str;
        }
        query_join = std::format("SELECT * FROM tbl {} {} ON {}", join_kind, str_right_table, str_attributes);
    }
    else
    {
        for (auto const & str : attribute_list)
            str_attributes = str_attributes.empty() ? str : str_attributes + "," + str;

        query_join = std::format("SELECT * FROM tbl {} {} USING {}", join_kind, str_right_table, str_attributes);
    }

    if (!parseSQLQueryByString(std::make_unique<ParserSelectQuery>(), query_join, sub_query_node, pos.max_depth))
        return false;

    ASTPtr table_expr;
    if (sub_query_node->as<ASTSelectQuery>()->tables()
        && sub_query_node->as<ASTSelectQuery>()->tables()->as<ASTTablesInSelectQuery>()->children.size() > 1)
    {
        table_expr = sub_query_node->as<ASTSelectQuery>()->tables()->as<ASTTablesInSelectQuery>()->children[1];
        if (table_expr->as<ASTTablesInSelectQueryElement>()->table_expression->as<ASTTableExpression>()->subquery)
            table_expr->as<ASTTablesInSelectQueryElement>()->table_expression->as<ASTTableExpression>()->subquery->as<ASTSubquery>()->alias
                = "right_";
        else if (table_expr->as<ASTTablesInSelectQueryElement>()->table_expression->as<ASTTableExpression>()->database_and_table_name)
        {
            table_expr
                = table_expr->as<ASTTablesInSelectQueryElement>()->table_expression->as<ASTTableExpression>()->database_and_table_name;
            if (auto * ast_with_alias = dynamic_cast<ASTWithAlias *>(table_expr.get()))
                ast_with_alias->alias = "right_";
        }
    }
    if (kql_join_kind == "innerunique")
    {
        if (!setSubQuerySource(sub_query_node, node, false, true, "left_"))
            return false;
    }
    else
    {
        if (!setSubQuerySource(sub_query_node, node, false, false, "left_"))
            return false;
    }

    node = std::move(sub_query_node);
    return true;
}

}
