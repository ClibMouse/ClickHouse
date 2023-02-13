#include "ParserKQLOperators.h"
#include "KustoFunctions/IParserKQLFunction.h"
#include "ParserKQLStatement.h"

#include <Parsers/CommonParsers.h>

#include <format>
#include <unordered_map>

namespace DB::ErrorCodes
{
extern const int SYNTAX_ERROR;
}

namespace
{
enum class WildcardsPos : uint8_t
{
    none,
    left,
    right,
    both
};

enum class KQLOperatorValue : uint16_t
{
    none,
    contains,
    not_contains,
    contains_cs,
    not_contains_cs,
    endswith,
    not_endswith,
    endswith_cs,
    not_endswith_cs,
    equal, /// =~
    not_equal, /// !~
    equal_cs, /// =
    not_equal_cs, /// !=
    has,
    not_has,
    has_all,
    has_any,
    has_cs,
    not_has_cs,
    hasprefix,
    not_hasprefix,
    hasprefix_cs,
    not_hasprefix_cs,
    hassuffix,
    not_hassuffix,
    hassuffix_cs,
    not_hassuffix_cs,
    in_cs, /// in
    not_in_cs, /// !in
    in, /// in~
    not_in, /// !in~
    matches_regex,
    startswith,
    not_startswith,
    startswith_cs,
    not_startswith_cs,
};

const std::unordered_map<String, KQLOperatorValue> KQLOperator = {
    {"contains", KQLOperatorValue::contains},
    {"!contains", KQLOperatorValue::not_contains},
    {"contains_cs", KQLOperatorValue::contains_cs},
    {"!contains_cs", KQLOperatorValue::not_contains_cs},
    {"endswith", KQLOperatorValue::endswith},
    {"!endswith", KQLOperatorValue::not_endswith},
    {"endswith_cs", KQLOperatorValue::endswith_cs},
    {"!endswith_cs", KQLOperatorValue::not_endswith_cs},
    {"=~", KQLOperatorValue::equal},
    {"!~", KQLOperatorValue::not_equal},
    {"==", KQLOperatorValue::equal_cs},
    {"!=", KQLOperatorValue::not_equal_cs},
    {"has", KQLOperatorValue::has},
    {"!has", KQLOperatorValue::not_has},
    {"has_all", KQLOperatorValue::has_all},
    {"has_any", KQLOperatorValue::has_any},
    {"has_cs", KQLOperatorValue::has_cs},
    {"!has_cs", KQLOperatorValue::not_has_cs},
    {"hasprefix", KQLOperatorValue::hasprefix},
    {"!hasprefix", KQLOperatorValue::not_hasprefix},
    {"hasprefix_cs", KQLOperatorValue::hasprefix_cs},
    {"!hasprefix_cs", KQLOperatorValue::not_hasprefix_cs},
    {"hassuffix", KQLOperatorValue::hassuffix},
    {"!hassuffix", KQLOperatorValue::not_hassuffix},
    {"hassuffix_cs", KQLOperatorValue::hassuffix_cs},
    {"!hassuffix_cs", KQLOperatorValue::not_hassuffix_cs},
    {"in", KQLOperatorValue::in_cs},
    {"!in", KQLOperatorValue::not_in_cs},
    {"in~", KQLOperatorValue::in},
    {"!in~", KQLOperatorValue::not_in},
    {"matches regex", KQLOperatorValue::matches_regex},
    {"startswith", KQLOperatorValue::startswith},
    {"!startswith", KQLOperatorValue::not_startswith},
    {"startswith_cs", KQLOperatorValue::startswith_cs},
    {"!startswith_cs", KQLOperatorValue::not_startswith_cs},
};
}

String genHasAnyAllOpExpr(
    std::vector<std::string> & tokens, DB::IParser::Pos & token_pos, const std::string & kql_op, const std::string_view ch_op)
{
    std::string new_expr;
    DB::Expected expected;
    DB::ParserToken s_lparen(DB::TokenType::OpeningRoundBracket);

    ++token_pos;
    if (!s_lparen.ignore(token_pos, expected))
        throw DB::Exception(DB::ErrorCodes::SYNTAX_ERROR, "Syntax error near {}", kql_op);

    auto haystack = tokens.back();
    const auto *const logic_op = (kql_op == "has_all") ? " and " : " or ";
    while (!token_pos->isEnd() && token_pos->type != DB::TokenType::PipeMark && token_pos->type != DB::TokenType::Semicolon)
    {
        auto tmp_arg = DB::IParserKQLFunction::getExpression(token_pos);
        if (token_pos->type == DB::TokenType::Comma)
            new_expr += logic_op;
        else
            new_expr += std::vformat(ch_op, std::make_format_args(haystack, tmp_arg));

        ++token_pos;
        if (token_pos->type == DB::TokenType::ClosingRoundBracket)
            break;
    }

    tokens.pop_back();
    return new_expr;
}

String genEqOpExprCis(std::vector<String> & tokens, DB::IParser::Pos & token_pos, const DB::String & ch_op)
{
    DB::String tmp_arg(token_pos->begin, token_pos->end);

    if (tokens.empty() || tmp_arg != "~")
        return tmp_arg;

    DB::String new_expr;
    new_expr += "lower(" + tokens.back() + ")";
    new_expr += ch_op;
    ++token_pos;
    new_expr += " lower(" + DB::String(token_pos->begin, token_pos->end) + ")" + " ";
    tokens.pop_back();

    return new_expr;
}

String genInOpExprCis(std::vector<String> & tokens, DB::IParser::Pos & token_pos, const DB::String & kql_op, const DB::String & ch_op)
{
    DB::ParserKQLTaleFunction kqlfun_p;

    DB::ParserToken s_lparen(DB::TokenType::OpeningRoundBracket);

    DB::ASTPtr select;
    DB::Expected expected;
    DB::String new_expr;

    ++token_pos;
    if (!s_lparen.ignore(token_pos, expected))
        throw DB::Exception("Syntax error near " + kql_op, DB::ErrorCodes::SYNTAX_ERROR);

    for (const auto & s : tokens)
        new_expr += "lower(" + s + ")" + " ";

    auto pos = token_pos;
    if (kqlfun_p.parse(pos, select, expected))
    {
        new_expr += ch_op + " kql";
        auto tmp_pos = token_pos;
        auto keep_pos = token_pos;
        int pipe = 0;
        bool desired_column_lowerd = false;
        while (tmp_pos != pos)
        {
            ++tmp_pos;
            if (tmp_pos->type == DB::TokenType::PipeMark)
                pipe += 1;
            if (pipe == 2 && !desired_column_lowerd)
            {
                new_expr = new_expr + " tolower(" + DB::String(keep_pos->begin, keep_pos->end) + ")";
                desired_column_lowerd = true;
            }
            else
                new_expr = new_expr + " " + DB::String(keep_pos->begin, keep_pos->end);
            ++keep_pos;
        }

        if (pos->type != DB::TokenType::ClosingRoundBracket)
            throw DB::Exception("Syntax error near " + kql_op, DB::ErrorCodes::SYNTAX_ERROR);

        token_pos = pos;
        tokens.pop_back();
        return new_expr;
    }
    --token_pos;
    --token_pos;

    new_expr += ch_op + "(";
    while (!token_pos->isEnd() && token_pos->type != DB::TokenType::PipeMark && token_pos->type != DB::TokenType::Semicolon)
    {
        auto tmp_arg = DB::String(token_pos->begin, token_pos->end);
        if (token_pos->type != DB::TokenType::Comma && token_pos->type != DB::TokenType::ClosingRoundBracket
            && token_pos->type != DB::TokenType::OpeningRoundBracket && token_pos->type != DB::TokenType::OpeningSquareBracket
            && token_pos->type != DB::TokenType::ClosingSquareBracket && tmp_arg != "~" && tmp_arg != "dynamic")
            new_expr = new_expr + "lower(" + tmp_arg + ")";
        ++token_pos;
        if (token_pos->type == DB::TokenType::ClosingRoundBracket)
            break;
        else if (token_pos->type == DB::TokenType::Comma)
            new_expr += ", ";
    }
    ++token_pos;
    new_expr += ")";

    tokens.pop_back();
    return new_expr;
}

std::string genInOpExpr(DB::IParser::Pos & token_pos, const std::string & kql_op, const std::string & ch_op)
{
    DB::ParserKQLTaleFunction kqlfun_p;
    DB::ParserToken s_lparen(DB::TokenType::OpeningRoundBracket);

    DB::ASTPtr select;
    DB::Expected expected;

    ++token_pos;
    if (!s_lparen.ignore(token_pos, expected))
        throw DB::Exception(DB::ErrorCodes::SYNTAX_ERROR, "Syntax error near {}", kql_op);

    auto pos = token_pos;
    if (kqlfun_p.parse(pos, select, expected))
    {
        auto new_expr = ch_op + " kql";
        auto tmp_pos = token_pos;
        while (tmp_pos != pos)
        {
            new_expr = new_expr + " " + std::string(tmp_pos->begin, tmp_pos->end);
            ++tmp_pos;
        }

        if (pos->type != DB::TokenType::ClosingRoundBracket)
            throw DB::Exception(DB::ErrorCodes::SYNTAX_ERROR, "Syntax error near {}", kql_op);

        token_pos = pos;
        return new_expr;
    }

    --token_pos;
    --token_pos;
    return ch_op;
}

std::string genHaystackOpExpr(
    std::vector<std::string> & tokens,
    DB::IParser::Pos & token_pos,
    const std::string & kql_op,
    const std::string_view ch_op,
    WildcardsPos wildcards_pos,
    WildcardsPos space_pos = WildcardsPos::none)
{
    std::string new_expr, left_wildcards, right_wildcards, left_space, right_space;

    switch (wildcards_pos)
    {
        case WildcardsPos::none:
            break;

        case WildcardsPos::left:
            left_wildcards = "%";
            break;

        case WildcardsPos::right:
            right_wildcards = "%";
            break;

        case WildcardsPos::both:
            left_wildcards = "%";
            right_wildcards = "%";
            break;
    }

    switch (space_pos)
    {
        case WildcardsPos::none:
            break;

        case WildcardsPos::left:
            left_space = " ";
            break;

        case WildcardsPos::right:
            right_space = " ";
            break;

        case WildcardsPos::both:
            left_space = " ";
            right_space = " ";
            break;
    }

    ++token_pos;

    if (!tokens.empty() && (token_pos->type == DB::TokenType::StringLiteral || token_pos->type == DB::TokenType::QuotedIdentifier))
        new_expr = std::vformat(
            ch_op,
            std::make_format_args(
                tokens.back(),
                "'" + left_wildcards + left_space + std::string(token_pos->begin + 1, token_pos->end - 1) + right_space + right_wildcards
                    + "'"));
    else if (!tokens.empty() && token_pos->type == DB::TokenType::BareWord)
    {
        auto tmp_arg = DB::IParserKQLFunction::getExpression(token_pos);
        new_expr = std::vformat(
            ch_op,
            std::make_format_args(
                tokens.back(), "concat('" + left_wildcards + left_space + "', " + tmp_arg + ", '" + right_space + right_wildcards + "')"));
    }
    else
        throw DB::Exception(DB::ErrorCodes::SYNTAX_ERROR, "Syntax error near {}", kql_op);

    tokens.pop_back();
    return new_expr;
}

namespace DB
{
bool KQLOperators::convert(std::vector<String> & tokens, IParser::Pos & pos)
{
    if (pos->isEnd() || pos->type == TokenType::PipeMark || pos->type == TokenType::Semicolon)
        return false;

    auto begin = pos;
    auto token = String(pos->begin, pos->end);

    String op = token;
    if (token == "!")
    {
        ++pos;
        if (pos->isEnd() || pos->type == TokenType::PipeMark || pos->type == TokenType::Semicolon)
            throw Exception("Invalid negative operator", ErrorCodes::SYNTAX_ERROR);
        op = "!" + String(pos->begin, pos->end);
    }
    else if (token == "matches")
    {
        ++pos;
        if (!pos->isEnd() && pos->type != TokenType::PipeMark && pos->type != TokenType::Semicolon)
        {
            if (String(pos->begin, pos->end) == "regex")
                op += " regex";
            else
                --pos;
        }
    }
    else
    {
        op = token;
    }

    ++pos;
    if (!pos->isEnd() && pos->type != TokenType::PipeMark && pos->type != TokenType::Semicolon)
    {
        if (String(pos->begin, pos->end) == "~")
            op += "~";
        else
            --pos;
    }
    else
        --pos;

    const auto op_it = KQLOperator.find(op);
    if (op_it == KQLOperator.end())
    {
        pos = begin;
        return false;
    }

    String new_expr;

    const auto & op_value = op_it->second;
    if (op_value == KQLOperatorValue::none)
    {
        tokens.push_back(op);
        return true;
    }

    if (tokens.empty())
        throw Exception("Syntax error near " + op, ErrorCodes::SYNTAX_ERROR);

    auto last_op = tokens.back();
    auto last_pos = pos;

    switch (op_value)
    {
        case KQLOperatorValue::contains:
            new_expr = genHaystackOpExpr(tokens, pos, op, "ilike({0}, {1})", WildcardsPos::both);
            break;

        case KQLOperatorValue::not_contains:
            new_expr = genHaystackOpExpr(tokens, pos, op, "not ilike({0}, {1})", WildcardsPos::both);
            break;

        case KQLOperatorValue::contains_cs:
            new_expr = genHaystackOpExpr(tokens, pos, op, "like({0}, {1})", WildcardsPos::both);
            break;

        case KQLOperatorValue::not_contains_cs:
            new_expr = genHaystackOpExpr(tokens, pos, op, "not like({0}, {1})", WildcardsPos::both);
            break;

        case KQLOperatorValue::endswith:
            new_expr = genHaystackOpExpr(tokens, pos, op, "ilike({0}, {1})", WildcardsPos::left);
            break;

        case KQLOperatorValue::not_endswith:
            new_expr = genHaystackOpExpr(tokens, pos, op, "not ilike({0}, {1})", WildcardsPos::left);
            break;

        case KQLOperatorValue::endswith_cs:
            new_expr = genHaystackOpExpr(tokens, pos, op, "endsWith({0}, {1})", WildcardsPos::none);
            break;

        case KQLOperatorValue::not_endswith_cs:
            new_expr = genHaystackOpExpr(tokens, pos, op, "not endsWith({0}, {1})", WildcardsPos::none);
            break;

        case KQLOperatorValue::equal:
            new_expr = genEqOpExprCis(tokens, pos, "==");
            break;

        case KQLOperatorValue::not_equal:
            new_expr = genEqOpExprCis(tokens, pos, "!=");
            break;

        case KQLOperatorValue::equal_cs:
            new_expr = "==";
            break;

        case KQLOperatorValue::not_equal_cs:
            new_expr = "!=";
            break;
        case KQLOperatorValue::has:
            new_expr = genHaystackOpExpr(tokens, pos, op, "ifNull(hasTokenCaseInsensitiveOrNull({0}, {1}), {0} = {1})", WildcardsPos::none);
            break;

        case KQLOperatorValue::not_has:
            new_expr
                = genHaystackOpExpr(tokens, pos, op, "not ifNull(hasTokenCaseInsensitiveOrNull({0}, {1}), {0} = {1})", WildcardsPos::none);
            break;

        case KQLOperatorValue::has_all:
            new_expr = genHasAnyAllOpExpr(tokens, pos, op, "ifNull(hasTokenCaseInsensitiveOrNull({0}, {1}), {0} = {1})");
            break;

        case KQLOperatorValue::has_any:
            new_expr = genHasAnyAllOpExpr(tokens, pos, op, "ifNull(hasTokenCaseInsensitiveOrNull({0}, {1}), {0} = {1})");
            break;

        case KQLOperatorValue::has_cs:
            new_expr = genHaystackOpExpr(tokens, pos, op, "ifNull(hasTokenOrNull({0}, {1}), {0} = {1})", WildcardsPos::none);
            break;

        case KQLOperatorValue::not_has_cs:
            new_expr = genHaystackOpExpr(tokens, pos, op, "not ifNull(hasTokenOrNull({0}, {1}), {0} = {1})", WildcardsPos::none);
            break;

        case KQLOperatorValue::hasprefix:
            new_expr = genHaystackOpExpr(tokens, pos, op, "ilike({0}, {1})", WildcardsPos::right);
            new_expr += " or ";
            tokens.push_back(last_op);
            new_expr += genHaystackOpExpr(tokens, last_pos, op, "ilike({0}, {1})", WildcardsPos::both, WildcardsPos::left);
            break;

        case KQLOperatorValue::not_hasprefix:
            new_expr = genHaystackOpExpr(tokens, pos, op, "not ilike({0}, {1})", WildcardsPos::right);
            new_expr += " and ";
            tokens.push_back(last_op);
            new_expr += genHaystackOpExpr(tokens, last_pos, op, "not ilike({0}, {1})", WildcardsPos::both, WildcardsPos::left);
            break;

        case KQLOperatorValue::hasprefix_cs:
            new_expr = genHaystackOpExpr(tokens, pos, op, "startsWith({0}, {1})", WildcardsPos::none);
            new_expr += " or ";
            tokens.push_back(last_op);
            new_expr += genHaystackOpExpr(tokens, last_pos, op, "like({0}, {1})", WildcardsPos::both, WildcardsPos::left);
            break;

        case KQLOperatorValue::not_hasprefix_cs:
            new_expr = genHaystackOpExpr(tokens, pos, op, "not startsWith({0}, {1})", WildcardsPos::none);
            new_expr += " and  ";
            tokens.push_back(last_op);
            new_expr += genHaystackOpExpr(tokens, last_pos, op, "not like({0}, {1})", WildcardsPos::both, WildcardsPos::left);
            break;

        case KQLOperatorValue::hassuffix:
            new_expr = genHaystackOpExpr(tokens, pos, op, "ilike({0}, {1})", WildcardsPos::left);
            new_expr += " or ";
            tokens.push_back(last_op);
            new_expr += genHaystackOpExpr(tokens, last_pos, op, "ilike({0}, {1})", WildcardsPos::both, WildcardsPos::right);
            break;

        case KQLOperatorValue::not_hassuffix:
            new_expr = genHaystackOpExpr(tokens, pos, op, "not ilike({0}, {1})", WildcardsPos::left);
            new_expr += " and ";
            tokens.push_back(last_op);
            new_expr += genHaystackOpExpr(tokens, last_pos, op, "not ilike({0}, {1})", WildcardsPos::both, WildcardsPos::right);
            break;

        case KQLOperatorValue::hassuffix_cs:
            new_expr = genHaystackOpExpr(tokens, pos, op, "endsWith({0}, {1})", WildcardsPos::none);
            new_expr += " or ";
            tokens.push_back(last_op);
            new_expr += genHaystackOpExpr(tokens, last_pos, op, "like({0}, {1})", WildcardsPos::both, WildcardsPos::right);
            break;

        case KQLOperatorValue::not_hassuffix_cs:
            new_expr = genHaystackOpExpr(tokens, pos, op, "not endsWith({0}, {1})", WildcardsPos::none);
            new_expr += " and  ";
            tokens.push_back(last_op);
            new_expr += genHaystackOpExpr(tokens, last_pos, op, "not like({0}, {1})", WildcardsPos::both, WildcardsPos::right);
            break;

        case KQLOperatorValue::in_cs:
            new_expr = genInOpExpr(pos, op, "in");
            break;

        case KQLOperatorValue::not_in_cs:
            new_expr = genInOpExpr(pos, op, "not in");
            break;

        case KQLOperatorValue::in:
            new_expr = genInOpExprCis(tokens, pos, op, "in");
            break;

        case KQLOperatorValue::not_in:
            new_expr = genInOpExprCis(tokens, pos, op, "not in");
            break;

        case KQLOperatorValue::matches_regex:
            new_expr = genHaystackOpExpr(tokens, pos, op, "match({0}, {1})", WildcardsPos::none);
            break;

        case KQLOperatorValue::startswith:
            new_expr = genHaystackOpExpr(tokens, pos, op, "ilike({0}, {1})", WildcardsPos::right);
            break;

        case KQLOperatorValue::not_startswith:
            new_expr = genHaystackOpExpr(tokens, pos, op, "not ilike({0}, {1})", WildcardsPos::right);
            break;

        case KQLOperatorValue::startswith_cs:
            new_expr = genHaystackOpExpr(tokens, pos, op, "startsWith({0}, {1})", WildcardsPos::none);
            break;

        case KQLOperatorValue::not_startswith_cs:
            new_expr = genHaystackOpExpr(tokens, pos, op, "not startsWith({0}, {1})", WildcardsPos::none);
            break;

        default:
            break;
    }

    tokens.push_back(new_expr);
    return true;
}
}
