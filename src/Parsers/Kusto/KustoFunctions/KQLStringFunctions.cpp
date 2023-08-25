#include "KQLStringFunctions.h"
#include "KQLFunctionFactory.h"

#include <Parsers/CommonParsers.h>

#include <format>

namespace DB::ErrorCodes
{
extern const int SYNTAX_ERROR;
extern const int BAD_ARGUMENTS;
}

namespace DB
{
bool Base64EncodeToString::convertImpl(String & out, IParser::Pos & pos)
{
    return directMapping(out, pos, "base64Encode");
}

bool Base64EncodeFromGuid::convertImpl(String & out, IParser::Pos & pos)
{
    const auto function_name = getKQLFunctionName(pos);
    if (function_name.empty())
        return false;

    const auto argument = getArgument(function_name, pos);
    out = std::format(
        "if(toTypeName({0}) not in ['UUID', 'Nullable(UUID)'], toString(throwIf(true, 'Expected guid as argument')), "
        "base64Encode(UUIDStringToNum(toString({0}), 2)))",
        argument,
        generateUniqueIdentifier());
    return true;
}

bool Base64DecodeToString::convertImpl(String & out, IParser::Pos & pos)
{
    const String fn_name = getKQLFunctionName(pos);
    if (fn_name.empty())
        return false;
    ++pos;
    const String str = getConvertedArgument(fn_name, pos);

    out = std::format(
        "IF ((length({0}) % 4) != 0, NULL, IF (countMatches(substring({0}, 1, length({0}) - 2), '=') > 0, NULL, IF(isValidUTF8(tryBase64Decode({0}) AS decoded_str_{1}),decoded_str_{1}, NULL)))",
        str,
        generateUniqueIdentifier());

    return true;
}

bool Base64DecodeToArray::convertImpl(String & out, IParser::Pos & pos)
{
    const String fn_name = getKQLFunctionName(pos);
    if (fn_name.empty())
        return false;

    ++pos;
    const String str = getConvertedArgument(fn_name, pos);

    out = std::format(
        "IF((length({0}) % 4) != 0, [NULL], IF(length(tryBase64Decode({0})) = 0, [NULL], IF(countMatches(substring({0}, 1, length({0}) - "
        "2), '=') > 0, [NULL], arrayMap(x -> reinterpretAsUInt8(x), splitByRegexp('', "
        "base64Decode(assumeNotNull(IF(length(tryBase64Decode({0})) = 0, '', {0}))))))))",
        str);

    return true;
}

bool Base64DecodeToGuid::convertImpl(String & out, IParser::Pos & pos)
{
    const auto function_name = getKQLFunctionName(pos);
    if (function_name.empty())
        return false;

    const auto argument = getArgument(function_name, pos);
    out = std::format("toUUIDOrNull(UUIDNumToString(toFixedString(base64Decode({}), 16), 2))", argument);

    return true;
}

bool CountOf::convertImpl(String & out, IParser::Pos & pos)
{
    const String fn_name = getKQLFunctionName(pos);
    if (fn_name.empty())
        return false;

    ++pos;
    const String source = getConvertedArgument(fn_name, pos);

    ++pos;
    const String search = getConvertedArgument(fn_name, pos);

    String kind = "'normal'";
    if (pos->type == TokenType::Comma)
    {
        ++pos;
        kind = getConvertedArgument(fn_name, pos);
    }
    assert(kind == "'normal'" || kind == "'regex'");

    if (kind == "'normal'")
        out = "kql_count_overlapping_substrings(" + source + ", " + search + ")";
    else
        out = "countMatches(" + source + ", " + search + ")";
    return true;
}

bool Extract::convertImpl(String & out, IParser::Pos & pos)
{
    ParserKeyword s_kql("typeof");
    ParserToken open_bracket(TokenType::OpeningRoundBracket);
    ParserToken close_bracket(TokenType::ClosingRoundBracket);
    Expected expected;

    std::unordered_map<String, String> type_cast
        = {{"bool", "Boolean"},
           {"boolean", "Boolean"},
           {"datetime", "DateTime"},
           {"date", "DateTime"},
           {"guid", "UUID"},
           {"int", "Int32"},
           {"long", "Int64"},
           {"real", "Float64"},
           {"double", "Float64"},
           {"string", "String"},
           {"decimal", "Decimal"}};

    const String fn_name = getKQLFunctionName(pos);
    if (fn_name.empty())
        return false;

    ++pos;
    String regex = getConvertedArgument(fn_name, pos);

    ++pos;
    String capture_group = getConvertedArgument(fn_name, pos);

    ++pos;
    String source = getConvertedArgument(fn_name, pos);

    String type_literal;

    if (pos->type == TokenType::Comma)
    {
        ++pos;

        if (s_kql.ignore(pos, expected))
        {
            if (!open_bracket.ignore(pos, expected))
                throw Exception(ErrorCodes::SYNTAX_ERROR, "Syntax error near typeof");

            type_literal = String(pos->begin, pos->end);

            if (type_cast.find(type_literal) == type_cast.end())
                throw Exception(ErrorCodes::UNKNOWN_TYPE, "{} is not a supported kusto data type for extract", type_literal);

            type_literal = type_cast[type_literal];
            ++pos;

            if (!close_bracket.ignore(pos, expected))
                throw Exception(ErrorCodes::SYNTAX_ERROR, "Syntax error near typeof");
        }
    }

    out = std::format("kql_extract({}, {}, {})", source, regex, capture_group);
    if (type_literal == "Decimal")
    {
        out = std::format("countSubstrings({0}, '.') > 1 ? NULL: {0}, length(substr({0}, position({0},'.') + 1)))", out);
        out = std::format("toDecimal128OrNull({0})", out);
    }
    else
    {
        if (type_literal == "Boolean")
            out = std::format("toInt64OrNull({})", out);

        if (!type_literal.empty())
            out = "accurateCastOrNull(" + out + ", '" + type_literal + "')";
    }
    return true;
}

bool ExtractAll::convertImpl(String & out, IParser::Pos & pos)
{
    const String fn_name = getKQLFunctionName(pos);
    if (fn_name.empty())
        return false;

    ++pos;
    const String regex = getConvertedArgument(fn_name, pos);

    ++pos;
    const String second_arg = getConvertedArgument(fn_name, pos);

    String third_arg;
    if (pos->type == TokenType::Comma)
    {
        ++pos;
        third_arg = getConvertedArgument(fn_name, pos);
        out = "arrayMap(x -> arrayFilter((y, i) -> i in " + second_arg + ", x, arrayEnumerate(x)), extractAllGroups(" + third_arg + ", "
            + regex + "))";
    }
    else
        out = "extractAllGroups(" + second_arg + ", " + regex + ")";
    return true;
}

bool ExtractJson::convertImpl(String & out, IParser::Pos & pos)
{
    String datatype = "String";
    ParserKeyword s_kql("typeof");
    ParserToken open_bracket(TokenType::OpeningRoundBracket);
    ParserToken close_bracket(TokenType::ClosingRoundBracket);
    Expected expected;

    std::unordered_map<String, String> type_cast
        = {{"bool", "Boolean"},
           {"boolean", "Boolean"},
           {"datetime", "DateTime"},
           {"date", "DateTime"},
           {"dynamic", "Array"},
           {"guid", "UUID"},
           {"int", "Int32"},
           {"long", "Int64"},
           {"real", "Float64"},
           {"double", "Float64"},
           {"string", "String"},
           {"decimal", "Decimal"}};

    const String fn_name = getKQLFunctionName(pos);
    if (fn_name.empty())
        return false;

    ++pos;
    const String json_datapath = getConvertedArgument(fn_name, pos);
    ++pos;
    const String json_datasource = getConvertedArgument(fn_name, pos);
    if (pos->type == TokenType::Comma)
    {
        ++pos;
        if (s_kql.ignore(pos, expected))
        {
            if (!open_bracket.ignore(pos, expected))
                throw Exception(ErrorCodes::SYNTAX_ERROR, "Syntax error near typeof");

            datatype = String(pos->begin, pos->end);

            if (type_cast.find(datatype) == type_cast.end())
                throw Exception(ErrorCodes::UNKNOWN_TYPE, "{} is not a supported kusto data type for {}", datatype, fn_name);
            datatype = type_cast[datatype];
            ++pos;

            if (!close_bracket.ignore(pos, expected))
                throw Exception(ErrorCodes::SYNTAX_ERROR, "Syntax error near typeof");
        }
    }
    const auto json_val = std::format("JSON_VALUE({0},{1})", json_datasource, json_datapath);
    if (datatype == "Decimal")
    {
        out = std::format("countSubstrings({0}, '.') > 1 ? NULL: length(substr({0}, position({0},'.') + 1)))", json_val);
        out = std::format("toDecimal128OrNull({0}::String, {1})", json_val, out);
    }
    else
    {
        if (datatype == "Boolean")
            out = std::format("if(toInt64OrNull({}) > 0, true, false)", json_val);
        else if (!datatype.empty())
            out = std::format("accurateCastOrNull({},'{}')", json_val, datatype);
    }
    return true;
}

bool HasAnyIndex::convertImpl(String & out, IParser::Pos & pos)
{
    const String fn_name = getKQLFunctionName(pos);
    if (fn_name.empty())
        return false;

    ++pos;
    const String source = getConvertedArgument(fn_name, pos);

    ++pos;
    const String lookup = getConvertedArgument(fn_name, pos);
    String src_array = std::format("splitByChar(' ',{})", source);
    out = std::format(
        "if (empty({1}), -1, indexOf(arrayMap(x -> (x in {0}), if (empty({1}), [''], arrayMap(x -> (toString(x)), {1}))), 1) - 1)",
        src_array,
        lookup);
    return true;
}

bool IndexOf::convertImpl(String & out, IParser::Pos & pos)
{
    const auto fn_name = getKQLFunctionName(pos);
    if (fn_name.empty())
        return false;

    const auto source = getArgument(fn_name, pos);
    const auto lookup = getArgument(fn_name, pos);
    const auto start_index = getOptionalArgument(fn_name, pos);
    const auto length = getOptionalArgument(fn_name, pos);
    const auto occurrence = getOptionalArgument(fn_name, pos);

    out = std::format(
        "kql_indexof(kql_tostring({}),kql_tostring({}),{},{},{})",
        source,
        lookup,
        start_index.value_or("0"),
        length.value_or("-1"),
        occurrence.value_or("1"));

    return true;
}

bool IndexOfRegex::convertImpl(String & out, IParser::Pos & pos)
{
    const auto fn_name = getKQLFunctionName(pos);
    if (fn_name.empty())
        return false;

    const auto source = getArgument(fn_name, pos);
    const auto lookup = getArgument(fn_name, pos);
    const auto start_index = getOptionalArgument(fn_name, pos);
    const auto length = getOptionalArgument(fn_name, pos);
    const auto occurrence = getOptionalArgument(fn_name, pos);

    out = std::format(
        "If(isNULL({0}), -1, kql_indexof_regex(kql_tostring({0}),kql_tostring({1}),{2},{3},{4}))",
        source,
        lookup,
        start_index.value_or("0"),
        length.value_or("-1"),
        occurrence.value_or("1"));

    return true;
}

bool IsAscii::convertImpl(String & out, IParser::Pos & pos)
{
    const String fn_name = getKQLFunctionName(pos);
    if (fn_name.empty())
        return false;
    ++pos;
    const auto arg = getConvertedArgument(fn_name, pos);
    out = std::format("not toBool(arrayExists(x -> x < 0 or x > 127, arrayMap(x -> ascii(x), splitByString('', assumeNotNull({})))))", arg);
    return true;
}

bool IsEmpty::convertImpl(String & out, IParser::Pos & pos)
{
    const String fn_name = getKQLFunctionName(pos);
    if (fn_name.empty())
        return false;
    const auto arg = getArgument(fn_name, pos, ArgumentState::Raw);
    out.append("empty(" + kqlCallToExpression("tostring", {arg}, pos.max_depth) + ")");
    return true;
}

bool IsNotEmpty::convertImpl(String & out, IParser::Pos & pos)
{
    const String fn_name = getKQLFunctionName(pos);
    if (fn_name.empty())
        return false;
    const auto arg = getArgument(fn_name, pos, ArgumentState::Raw);
    out.append("notEmpty(" + kqlCallToExpression("tostring", {arg}, pos.max_depth) + ")");
    return true;
}

bool IsNotNull::convertImpl(String & out, IParser::Pos & pos)
{
    return directMapping(out, pos, "isNotNull");
}

bool ParseCommandLine::convertImpl(String & out, IParser::Pos & pos)
{
    const String fn_name = getKQLFunctionName(pos);
    if (fn_name.empty())
        return false;

    ++pos;
    const String json_string = getConvertedArgument(fn_name, pos);

    ++pos;
    const String type = getConvertedArgument(fn_name, pos);

    if (type != "'windows'")
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Supported type argument is windows for {}", fn_name);

    out = std::format(
        "if(empty({0}) OR hasAll(splitByChar(' ', {0}) , ['']) , arrayMap(x->null, splitByChar(' ', '')), splitByChar(' ', {0}))",
        json_string);
    return true;
}

bool IsUtf8::convertImpl(String & out, IParser::Pos & pos)
{
    return directMapping(out, pos, "isValidUTF8");
}

bool IsNull::convertImpl(String & out, IParser::Pos & pos)
{
    return directMapping(out, pos, "isNull");
}

bool MakeString::convertImpl(String & out, IParser::Pos & pos)
{
    return directMapping(out, pos, "kql_make_string");
}

bool NewGuid::convertImpl(String & out, IParser::Pos & pos)
{
    return directMapping(out, pos, "generateUUIDv4", {0, 0});
}

bool ParseCSV::convertImpl(String & out, IParser::Pos & pos)
{
    const String fn_name = getKQLFunctionName(pos);
    if (fn_name.empty())
        return false;

    ++pos;
    String csv_string = getConvertedArgument(fn_name, pos);

    out = std::format(
        "if(position({0} ,'\n')::UInt8, (splitByChar(',', substring({0}, 1, position({0},'\n') -1))), (splitByChar(',', substring({0}, 1, "
        "length({0})))))",
        csv_string);
    return true;
}

bool ParseJson::convertImpl(String & out, IParser::Pos & pos)
{
    const String fn_name = getKQLFunctionName(pos);
    if (fn_name.empty())
        return false;

    ++pos;
    if (String(pos->begin, pos->end) == "dynamic")
    {
        --pos;
        auto arg = getArgument(fn_name, pos);
        auto result = kqlCallToExpression("dynamic", {arg}, pos.max_depth);
        out = std::format("{}", result);
    }
    else
    {
        auto arg = getConvertedArgument(fn_name, pos);
        out = std::format("if (isValidJSON({0}) , JSON_QUERY({0}, '$') , toJSONString({0}))", arg);
    }
    return true;
}

bool ParseURL::convertImpl(String & out, IParser::Pos & pos)
{
    return directMapping(out, pos, "kql_parseurl");
}

bool ParseURLQuery::convertImpl(String & out, IParser::Pos & pos)
{
    const String fn_name = getKQLFunctionName(pos);
    if (fn_name.empty())
        return false;
    ++pos;
    const String query = getConvertedArgument(fn_name, pos);

    const String query_string = std::format("if (position({},'?') > 0, queryString({}), {})", query, query, query);
    const String query_parameters
        = std::format(R"(concat('"Query Parameters":', concat('{{"', replace(replace({}, '=', '":"'),'&','","') ,'"}}')))", query_string);
    out = std::format("concat('{{',{},'}}')", query_parameters);
    return true;
}

bool ParseVersion::convertImpl(String & out, IParser::Pos & pos)
{
    const String fn_name = getKQLFunctionName(pos);
    if (fn_name.empty())
        return false;
    String arg;
    ++pos;
    arg = getConvertedArgument(fn_name, pos);
    out = std::format(
        "length(splitByChar('.', {0})) > 4 OR  length(splitByChar('.', {0})) < 1 OR match({0}, '.*[a-zA-Z]+.*') = 1 OR empty({0}) OR "
        "hasAll(splitByChar('.', {0}) , ['']) ? toDecimal128OrNull('NULL' , 0)  : "
        "toDecimal128OrNull(substring(arrayStringConcat(arrayMap(x -> leftPad(x, 8, '0'), arrayMap(x -> if(empty(x), '0', x), "
        "arrayResize(splitByChar('.', {0}), 4)))), 8),0)",
        arg);
    return true;
}

bool ReplaceRegex::convertImpl(String & out, IParser::Pos & pos)
{
    return directMapping(out, pos, "replaceRegexpAll");
}

bool Reverse::convertImpl(String & out, IParser::Pos & pos)
{
    const auto function_name = getKQLFunctionName(pos);
    if (function_name.empty())
        return false;

    const auto argument = getArgument(function_name, pos, ArgumentState::Raw);
    out = std::format("reverse({})", kqlCallToExpression("tostring", {argument}, pos.max_depth));

    return true;
}

bool Split::convertImpl(String & out, IParser::Pos & pos)
{
    const String fn_name = getKQLFunctionName(pos);
    if (fn_name.empty())
        return false;

    ++pos;
    const String source = getConvertedArgument(fn_name, pos);

    ++pos;
    const String delimiter = getConvertedArgument(fn_name, pos);
    auto split_res = std::format("empty({0}) ? splitByString(' ' , {1}) : splitByString({0} , {1})", delimiter, source);
    int requested_index = -1;

    if (pos->type == TokenType::Comma)
    {
        ++pos;
        auto arg = getConvertedArgument(fn_name, pos);
        // remove space between minus and value
        arg.erase(remove_if(arg.begin(), arg.end(), isspace), arg.end());
        requested_index = std::stoi(arg);
        requested_index += 1;
        out = std::format(
            "multiIf(length({0}) >= {1} AND {1} > 0 , arrayPushBack([],arrayElement({0}, {1})) , {1}=0 ,{0} , arrayPushBack([] "
            ",arrayElement(NULL,1)))",
            split_res,
            requested_index);
    }
    else
        out = split_res;
    return true;
}

bool StrCat::convertImpl(String & out, IParser::Pos & pos)
{
    const auto function_name = getKQLFunctionName(pos);
    if (function_name.empty())
        return false;

    const auto arguments = getArguments(function_name, pos, ArgumentState::Raw);

    out.append("concat(");
    for (const auto & argument : arguments)
    {
        out.append(kqlCallToExpression("tostring", {argument}, pos.max_depth));
        out.append(", ");
    }

    out.append("'')");
    return true;
}

bool StrCatDelim::convertImpl(String & out, IParser::Pos & pos)
{
    const String fn_name = getKQLFunctionName(pos);
    if (fn_name.empty())
        return false;

    const auto arguments = getArguments(fn_name, pos, ArgumentState::Raw, {2, 64});
    const String & delimiter = arguments[0];

    String args;
    args = "concat(";
    for (size_t i = 1; i < arguments.size(); i++)
    {
        args += kqlCallToExpression("tostring", {arguments[i]}, pos.max_depth);
        if (i < arguments.size() - 1)
            args += ", " + delimiter + ", ";
    }
    args += ")";
    out = std::move(args);
    return true;
}

bool StrCmp::convertImpl(String & out, IParser::Pos & pos)
{
    const String fn_name = getKQLFunctionName(pos);
    if (fn_name.empty())
        return false;

    ++pos;
    const String string1 = getConvertedArgument(fn_name, pos);
    ++pos;
    const String string2 = getConvertedArgument(fn_name, pos);

    out = std::format("multiIf({0} == {1}, 0, {0} < {1}, -1, 1)", string1, string2);
    return true;
}

bool StringSize::convertImpl(String & out, IParser::Pos & pos)
{
    return directMapping(out, pos, "length");
}

bool StrLen::convertImpl(String & out, IParser::Pos & pos)
{
    return directMapping(out, pos, "lengthUTF8");
}

bool StrRep::convertImpl(String & out, IParser::Pos & pos)
{
    const String fn_name = getKQLFunctionName(pos);

    if (fn_name.empty())
        return false;

    const auto arguments = getArguments(fn_name, pos, ArgumentState::Raw, {2, 3});
    const String & value = arguments[0];
    const String & multiplier = arguments[1];

    if (arguments.size() == 2)
        out = "repeat(" + value + " , " + multiplier + ")";
    else if (arguments.size() == 3)
    {
        const String & delimiter = arguments[2];
        const String repeated_str
            = "repeat(concat(" + kqlCallToExpression("tostring", {value}, pos.max_depth) + " , " + delimiter + ")," + multiplier + ")";
        out = "substr(" + repeated_str + ", 1, length(" + repeated_str + ") - length(" + delimiter + "))";
    }
    return true;
}

bool SubString::convertImpl(String & out, IParser::Pos & pos)
{
    String fn_name = getKQLFunctionName(pos);

    if (fn_name.empty())
        return false;

    ++pos;
    String source = getConvertedArgument(fn_name, pos);

    ++pos;
    String starting_index = getConvertedArgument(fn_name, pos);

    if (pos->type == TokenType::Comma)
    {
        ++pos;
        auto length = getConvertedArgument(fn_name, pos);

        if (starting_index.empty())
            throw Exception(ErrorCodes::SYNTAX_ERROR, "number of arguments do not match in function: {}", fn_name);
        else
            out = "if(toInt64(length(" + source + ")) <= 0, '', substr(" + source + ", " + "((" + starting_index + "% toInt64(length("
                + source + "))  + toInt64(length(" + source + "))) % toInt64(length(" + source + ")))  + 1, " + length + ") )";
    }
    else
        out = "if(toInt64(length(" + source + ")) <= 0, '', substr(" + source + "," + "((" + starting_index + "% toInt64(length(" + source
            + ")) + toInt64(length(" + source + "))) % toInt64(length(" + source + "))) + 1))";

    return true;
}

bool ToLower::convertImpl(String & out, IParser::Pos & pos)
{
    return directMapping(out, pos, "lower");
}

bool ToUpper::convertImpl(String & out, IParser::Pos & pos)
{
    return directMapping(out, pos, "upper");
}

bool ToUtf8::convertImpl(String & out, IParser::Pos & pos)
{
    String fn_name = getKQLFunctionName(pos);

    if (fn_name.empty())
        return false;

    ++pos;
    String func_arg = getConvertedArgument(fn_name, pos);
    const String base_arg = "reinterpretAsInt64(reverse(UNBIN(";
    const String base_arg_end = ")))";
    const String expr0 = base_arg + "substring(bin(x),2,7)" + base_arg_end;
    const String expr1 = base_arg + "concat(substring(bin(x),4,5), substring(bin(x),11,6))" + base_arg_end;
    const String expr2 = base_arg + "concat(substring(bin(x),5,4), substring(bin(x),11,6), substring(bin(x),19,6))" + base_arg_end;
    const String expr3
        = base_arg + "concat(substring(bin(x),6,3), substring(bin(x),11,6), substring(bin(x),19,6), substring(bin(x),27,6))" + base_arg_end;

    out = std::format(
        "arrayMap(x -> if(substring(bin(x),1,1)=='0', {0},"
        "if (substring(bin(x),1,3)=='110', {1},if(substring(bin(x),1,4)=='1110'"
        ", {2},if (substring(bin(x),1,5)=='11110', {3},-1)))), ngrams({4}, 1))",
        expr0,
        expr1,
        expr2,
        expr3,
        func_arg);
    return true;
}

bool Translate::convertImpl(String & out, IParser::Pos & pos)
{
    const String fn_name = getKQLFunctionName(pos);

    if (fn_name.empty())
        return false;

    ++pos;
    String from = getConvertedArgument(fn_name, pos);
    ++pos;
    String to = getConvertedArgument(fn_name, pos);
    ++pos;
    String source = getConvertedArgument(fn_name, pos);

    String len_diff = std::format("length({}) - length({})", from, to);
    String to_str = std::format(
        "multiIf(length({1}) = 0, {0}, {2} > 0, concat({1},repeat(substr({1},length({1}),1),toUInt16({2}))),{2} < 0 , "
        "substr({1},1,length({0})),{1})",
        from,
        to,
        len_diff);
    out = std::format("if (length({3}) = 0,'',translate({0},{1},{2}))", source, from, to_str, to);
    return true;
}

bool Trim::convertImpl(String & out, IParser::Pos & pos)
{
    const String fn_name = getKQLFunctionName(pos);
    if (fn_name.empty())
        return false;

    const auto regex = getArgument(fn_name, pos, ArgumentState::Raw);
    const auto source = getArgument(fn_name, pos, ArgumentState::Raw);
    out = kqlCallToExpression("trim_start", {regex, std::format("trim_end({0}, {1})", regex, source)}, pos.max_depth);

    return true;
}

bool TrimEnd::convertImpl(String & out, IParser::Pos & pos)
{
    const String fn_name = getKQLFunctionName(pos);
    if (fn_name.empty())
        return false;

    const auto regex = getArgument(fn_name, pos);
    const auto source = getArgument(fn_name, pos);
    out = std::format("replaceRegexpOne({0}, concat({1}, '$'), '')", source, regex);

    return true;
}

bool TrimStart::convertImpl(String & out, IParser::Pos & pos)
{
    const String fn_name = getKQLFunctionName(pos);
    if (fn_name.empty())
        return false;

    const auto regex = getArgument(fn_name, pos);
    const auto source = getArgument(fn_name, pos);
    out = std::format("replaceRegexpOne({0}, concat('^', {1}), '')", source, regex);

    return true;
}

bool URLDecode::convertImpl(String & out, IParser::Pos & pos)
{
    return directMapping(out, pos, "decodeURLComponent");
}

bool URLEncode::convertImpl(String & out, IParser::Pos & pos)
{
    return directMapping(out, pos, "encodeURLComponent");
}

}
