#include <Columns/ColumnString.h>
#include <DataTypes/DataTypeString.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>
#include <boost/spirit/home/x3.hpp>

#include <format>

namespace x3 = boost::spirit::x3;

namespace
{
using x3::char_;
using x3::lexeme;
using x3::lit;

struct KQLURLparts
{
    std::string schema = "";
    std::string user = "";
    std::string pass = "";
    std::string host = "";
    std::string port = "";
    std::string path = "";
    std::string args = "{";
    std::string frag = "";
    bool first = false;
};

const auto schema = lit("://");
const auto colon = lit(":");
const auto at = lit("@");
const auto slash = lit("/");
const auto equals = lit("=");
const auto fragmark = lit("#");
const auto openbracket = lit("[");
const auto closebracket = lit("]");

const auto endhost = char_(":/?#");
const auto endauth = char_("@/?#");
const auto endpath = char_("?#");
const auto querymark = char_("?&");
const auto endarg = char_("#&");

const auto set_schema = [](auto & ctx) { _val(ctx).schema = _attr(ctx); };
const auto set_user = [](auto & ctx) { _val(ctx).user = _attr(ctx); };
const auto set_pass = [](auto & ctx) { _val(ctx).pass = _attr(ctx); };
const auto set_host = [](auto & ctx) { _val(ctx).host = _attr(ctx); };
const auto set_port = [](auto & ctx) { _val(ctx).port = _attr(ctx); };
const auto set_path = [](auto & ctx) { _val(ctx).path = _attr(ctx); };
const auto set_args = [](auto & ctx)
{
    _val(ctx).args.append(_val(ctx).first ? "," : "");
    _val(ctx).args.append("\"" + _attr(ctx) + "\"");
    _val(ctx).first = true;
};
const auto set_argval = [](auto & ctx) { _val(ctx).args.append(":\"" + _attr(ctx) + "\""); };
const auto set_frag = [](auto & ctx) { _val(ctx).frag = _attr(ctx); };

const auto KQL_URL_SCHEMA_def = lexeme[+(char_ - schema) >> schema][set_schema];
const auto KQL_URL_USER_def = lexeme[(+(char_ - colon) >> &colon) >> &colon >> &(+(char_ - endauth) >> &at)][set_user];
const auto KQL_URL_PASS_def = lexeme[colon >> +(char_ - endauth) >> at][set_pass];
const auto KQL_URL_IPV6_def = lexeme[openbracket >> +(char_ - closebracket) >> closebracket][set_host];
const auto KQL_URL_HOST_def = lexeme[+(char_ - endhost)][set_host];
const auto KQL_URL_PORT_def = lexeme[colon >> +(char_ - endhost)][set_port];
const auto KQL_URL_PATH_def = lexeme[&slash >> +(char_ - endpath)][set_path];
const auto KQL_URL_ARGS_def = lexeme[+(querymark >> (+(char_ - equals) >> equals)[set_args] >> (+(char_ - endarg))[set_argval])];
const auto KQL_URL_FRAG_def = lexeme[fragmark >> +(char_)][set_frag];

const x3::rule<class KQLURL, KQLURLparts> KQL_URL = "KQL URL";
const auto KQL_URL_def = KQL_URL_SCHEMA_def >> -KQL_URL_USER_def >> -KQL_URL_PASS_def >> -KQL_URL_IPV6_def >> -KQL_URL_HOST_def
    >> -KQL_URL_PORT_def >> -KQL_URL_PATH_def >> -KQL_URL_ARGS_def >> -KQL_URL_FRAG_def;

BOOST_SPIRIT_DEFINE(KQL_URL);
}

namespace DB
{
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

    std::string outstr;

    for (size_t i = 0; i < input_rows_count; ++i)
    {
        std::string instr = arguments[0].column->getDataAt(i).toString();
        KQLURLparts url;
        parse(instr.begin(), instr.end(), KQL_URL, url);
        url.args.append("}");
        outstr = std::format(
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
        result->insertData(outstr.c_str(), outstr.size());
    }
    return result;
}

REGISTER_FUNCTION(KqlParseURL)
{
    factory.registerFunction<FunctionKqlParseURL>();
}
}
