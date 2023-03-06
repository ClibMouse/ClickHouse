#include <Parsers/CommonParsers.h>
#include <base/find_symbols.h>

namespace DB
{

bool ParserKeyword::parseImpl(Pos & pos, [[maybe_unused]] ASTPtr & node, Expected & expected)
{
    if (pos->type != TokenType::BareWord)
        return false;

    const char * current_word = s.begin();

    while (true)
    {
        expected.add(pos, current_word);

        if (pos->type != TokenType::BareWord)
            return false;

        const char * const next_whitespace = find_first_symbols<' ', '\0'>(current_word, s.end());
        const size_t word_length = next_whitespace - current_word;

        if (word_length != pos->size())
            return false;

        if (0 != strncasecmp(pos->begin, current_word, word_length))
            return false;

        ++pos;

        if (!*next_whitespace)
            break;

        current_word = next_whitespace + 1;
    }

    return true;
}

bool ParserSequence::parseImpl(Pos & pos, [[maybe_unused]] ASTPtr & node, Expected & expected)
{
    expected.add(pos, sequence.c_str());

    Tokens keyword_tokens(sequence.c_str(), sequence.c_str() + sequence.length());
    Pos keyword_tokens_pos(keyword_tokens, pos.max_depth);

    while (!keyword_tokens_pos->isEnd() && !pos->isEnd() && keyword_tokens_pos->type == pos->type)
    {
        const auto keyword_token_length = keyword_tokens_pos->end - keyword_tokens_pos->begin;

        if (const auto pos_token_length = pos->end - pos->begin;
            keyword_token_length != pos_token_length || strncasecmp(keyword_tokens_pos->begin, pos->begin, keyword_token_length) != 0)
            break;

        ++keyword_tokens_pos;
        ++pos;
    }

    return keyword_tokens_pos->isEnd();
}

}
