#include <Columns/ColumnString.h>
#include <DataTypes/DataTypeString.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>
#include <boost/fusion/adapted/std_pair.hpp>
#include <boost/spirit/home/x3.hpp>

#include <format>

namespace x3 = boost::spirit::x3;

namespace
{
using x3::char_;
using x3::lexeme;
using x3::lit;

struct KQLURLstate
{
    std::string schema;
    std::string user;
    std::string pass;
    std::string host;
    std::string port;
    std::string path;
    std::string args = "{";
    std::string frag;
};

const auto endschema = lit("://");
const auto colon = lit(":");
const auto at = lit("@");
const auto slash = lit("/");
const auto equals = lit("=");
const auto fragmark = lit("#");
const auto openbracket = lit("[");
const auto closebracket = lit("]");
const auto question = lit("?");
const auto ampersand = lit("&");

const auto endhost = char_("/:?#");
const auto endport = char_("/?#");
const auto endauth = char_("@/?#");
const auto endpath = char_("?#");
const auto endarg = char_("#&");

const auto set_schema = [](auto & ctx) { _val(ctx).schema = _attr(ctx); };
const auto set_auth = [](auto & ctx)
{
    const auto & auth = _attr(ctx);
    _val(ctx).user = at_c<0>(auth);
    _val(ctx).pass = at_c<1>(auth);
};
const auto set_host = [](auto & ctx) { _val(ctx).host = _attr(ctx); };
const auto set_port = [](auto & ctx) { _val(ctx).port = _attr(ctx); };
const auto set_path = [](auto & ctx) { _val(ctx).path = _attr(ctx); };
const auto set_args = [](auto & ctx)
{
    bool first = false;
    for (auto q_iter = _attr(ctx).begin(); q_iter < _attr(ctx).end(); ++q_iter)
    {
        _val(ctx).args.append((first ? ",\"" : "\"") + q_iter->first + "\":\"" + q_iter->second + "\"");
        first = true;
    }
};
const auto set_frag = [](auto & ctx) { _val(ctx).frag = _attr(ctx); };

template <typename T>
auto as = [](auto p) { return x3::rule<struct _, T>{} = as_parser(p); };

const auto KQL_URL_SCHEMA_def = lexeme[+(char_ - endschema) >> endschema][set_schema];
const auto KQL_URL_AUTH_def = lexeme[(+(char_ - colon) >> &colon) >> colon >> (+(char_ - endauth) >> at)][set_auth];
const auto KQL_URL_HOST_def
    = lexeme[as<std::string>((openbracket >> +(char_ - closebracket) >> closebracket) | (+(char_ - endhost)))][set_host];
const auto KQL_URL_PORT_def = lexeme[colon >> +(char_ - endport)][set_port];
const auto KQL_URL_PATH_def = lexeme[&slash >> +(char_ - endpath)][set_path];
const auto KQL_URL_ARGS_def = lexeme[as<std::vector<std::pair<std::string, std::string>>>(
    +((question | ampersand) >> (+(char_ - equals) >> equals) >> (+(char_ - endarg))))][set_args];
const auto KQL_URL_FRAG_def = lexeme[fragmark >> +(char_)][set_frag];

const x3::rule<class KQLURL, KQLURLstate> KQL_URL = "KQL URL";
const auto KQL_URL_def = KQL_URL_SCHEMA_def >> -KQL_URL_AUTH_def >> -KQL_URL_HOST_def >> -KQL_URL_PORT_def >> -KQL_URL_PATH_def
    >> -KQL_URL_ARGS_def >> -KQL_URL_FRAG_def;

BOOST_SPIRIT_DEFINE(KQL_URL);
}

namespace DB
{
namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

class FunctionKqlParseURL : public IFunction
{
public:
    static constexpr auto name = "kql_parseurl";
    static FunctionPtr create(ContextPtr context) { return std::make_shared<FunctionKqlParseURL>(std::move(context)); }

    explicit FunctionKqlParseURL(ContextPtr context_) : context(std::move(context_)) { }
    ~FunctionKqlParseURL() override = default;

    ColumnPtr
    executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override;
    String getName() const override { return name; }
    size_t getNumberOfArguments() const override { return 1; }
    DataTypePtr getReturnTypeImpl(const DataTypes &) const override { return std::make_shared<DataTypeString>(); }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }

private:
    ContextPtr context;
};

ColumnPtr
FunctionKqlParseURL::executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, const size_t input_rows_count) const
{
    auto result = ColumnString::create();

    if (!isStringOrFixedString(arguments.at(0).type))
        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "parse_url(): argument #1 - invalid data type: string");

    for (size_t i = 0; i < input_rows_count; ++i)
    {
        const auto in_str = arguments[0].column->getDataAt(i).toView();
        KQLURLstate url;
        parse(in_str.begin(), in_str.end(), KQL_URL, url);
        url.args.append("}");
        const auto out_str = std::format(
            "{}\"Scheme\":\"{}\",\"Host\":\"{}\",\"Port\":\"{}\",\"Path\":\"{}\",\"Username\":\"{}\",\"Password\":\"{}\",\"Query "
            "Parameters\":{},\"Fragment\":\"{}\"{}",
            "{",
            url.schema,
            url.host,
            url.port,
            url.path,
            url.user,
            url.pass,
            url.args,
            url.frag,
            "}");
        result->insertData(out_str.c_str(), out_str.size());
    }
    return result;
}

REGISTER_FUNCTION(KqlParseURL)
{
    factory.registerFunction<FunctionKqlParseURL>();
}
}
