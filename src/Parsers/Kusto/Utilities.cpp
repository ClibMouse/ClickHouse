#include "Utilities.h"

#include "KustoFunctions/IParserKQLFunction.h"

namespace DB
{
String extractLiteralArgumentWithoutQuotes(const std::string & function_name, IParser::Pos & pos)
{
    ++pos;
    if (pos->type == TokenType::QuotedIdentifier || pos->type == TokenType::StringLiteral)
    {
        auto result = extractTokenWithoutQuotes(pos);
        ++pos;
        return result;
    }

    --pos;
    return IParserKQLFunction::getArgument(function_name, pos, IParserKQLFunction::ArgumentState::Raw);
}

String extractTokenWithoutQuotes(IParser::Pos & pos)
{
    const auto offset = static_cast<int>(pos->type == TokenType::QuotedIdentifier || pos->type == TokenType::StringLiteral);
    return {pos->begin + offset, pos->end - offset};
}

String WildcardToRegex(const String & wildcard)
{
    String regex;
    for (char c : wildcard)
    {
        if (c == '*')
        {
            regex += ".*";
        }
        else if (c == '?')
        {
            regex += ".";
        }
        else if (c == '.' || c == '+' || c == '(' || c == ')' || c == '[' || c == ']' || c == '\\' || c == '^' || c == '$')
        {
            regex += "\\";
            regex += c;
        }
        else
        {
            regex += c;
        }
    }
    return regex;
}

}
