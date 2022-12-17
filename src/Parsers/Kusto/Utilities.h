#include <Parsers/IParser.h>

namespace DB
{
String extractLiteralArgumentWithoutQuotes(const std::string & function_name, IParser::Pos & pos);
String extractTokenWithoutQuotes(IParser::Pos & pos);
}
