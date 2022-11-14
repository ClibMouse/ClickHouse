#include "Utilities.h"

namespace DB
{
String extractTokenWithoutQuotes(IParser::Pos & pos)
{
    const auto offset = static_cast<int>(pos->type == TokenType::QuotedIdentifier || pos->type == TokenType::StringLiteral);
    return {pos->begin + offset, pos->end - offset};
}

bool isValidKQLPos(IParser::Pos & pos)
{
    return (pos.isValid() ||
            pos->type == TokenType::ErrorSingleExclamationMark || // allow kql negative operators
            pos->type == TokenType::ErrorWrongNumber || // allow kql timespan data type with decimal like 2.6h
            std::string_view(pos->begin, pos->end) == "~");  // allow kql Case-Sensitive operators
}
}
