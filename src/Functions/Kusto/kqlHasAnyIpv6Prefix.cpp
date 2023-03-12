#include <Columns/ColumnArray.h>
#include <Columns/ColumnDecimal.h>
#include <Columns/ColumnString.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>
#include <Functions/Kusto/KqlFunctionBase.h>
#include <Functions/Kusto/kqlHasAnyIp.h>
#include <boost/spirit/home/x3.hpp>
#include <ranges>
#include <regex>
#include <format>

static std::string ipv6ToHex(const std::string & str, const DB::DataTypePtr & result_type, const DB::ContextPtr & context)
{
    const DB::ColumnsWithTypeAndName ipv6_string = {DB::createConstColumnWithTypeAndName<DB::DataTypeString>(str, "ipv6")};
    const auto is_ipv6
        = DB::FunctionFactory::instance().get("isIPv6String", context)->build(ipv6_string)->execute(ipv6_string, result_type, 1);
    if (is_ipv6->getUInt(0))
    {
        const auto ipv6_string_to_num = DB::executeFunctionCall(context, "IPv6StringToNum", ipv6_string, 1);
        const DB::ColumnsWithTypeAndName hex_args{DB::asArgument(ipv6_string_to_num, "")};
        const auto [hex_string, _] = DB::executeFunctionCall(context, "hex", hex_args, 1);
        return hex_string->getDataAt(0).toString();
    }
    return "";
}

static std::string ipv6PrefixToHex(const std::string & str, const DB::DataTypePtr & result_type, const DB::ContextPtr & context)
{
    std::string ipv6_hex = "";
    std::vector<uint32_t> vec_v6;
    std::vector<uint32_t> vec_v4;
    const auto last_char = str.back();

    if (last_char == ':')
    {
        const auto ipv6 = boost::spirit::x3::hex % ':';
        const auto r = boost::spirit::x3::parse(str.begin(), str.end(), ipv6, vec_v6);
        if (!r || vec_v6.empty() || vec_v6.size() > 7)
        {
            return "";
        }
    }
    else if (last_char == '.')
    {
        const auto ipv6_prefix = boost::spirit::x3::hex >> ':' >> boost::spirit::x3::hex >> ':' >> boost::spirit::x3::hex >> ':'
            >> boost::spirit::x3::hex >> ':' >> boost::spirit::x3::hex >> ':' >> boost::spirit::x3::hex >> ':';
        const auto ipv4_embedded = boost::spirit::x3::uint_ % '.';

        auto iter = str.begin();
        auto iter_end = str.end();

        auto r = boost::spirit::x3::parse(iter, iter_end, ipv6_prefix, vec_v6);
        if (!r || vec_v6.empty() || vec_v6.size() != 6)
        {
            return "";
        }

        r = boost::spirit::x3::parse(iter, iter_end, ipv4_embedded, vec_v4);

        if (!r || vec_v4.empty() || vec_v4.size() >= 4)
        {
            return "";
        }
    }
    else
    {
        return ipv6ToHex(str, result_type, context);
    }

    if (std::ranges::any_of(vec_v6, [](const auto & x) { return x > 65535; }) || std::ranges::any_of(vec_v4, [](const auto & x) { return x > 255; }))
    {
        return "";
    }

    std::all_of(
        vec_v6.begin(),
        vec_v6.end(),
        [&ipv6_hex](const auto & n)
        {
            ipv6_hex += std::format("{:04X}", n);
            return 1;
        });
    std::all_of(
        vec_v4.begin(),
        vec_v4.end(),
        [&ipv6_hex](const auto & n)
        {
            ipv6_hex += std::format("{:02X}", n);
            return 1;
        });

    return ipv6_hex;
}

static std::vector<std::string> extractIpsFromArguments(
    const DB::ColumnsWithTypeAndName & arguments, const DB::DataTypePtr & result_type, const DB::ContextPtr & context, size_t row)
{
    std::vector<std::string> ips;

    if (DB::isStringOrFixedString(arguments.at(1).type))
    {
        std::ranges::copy_if(
            arguments | std::views::drop(1)
                | std::views::transform([&row, &result_type, &context](const DB::ColumnWithTypeAndName & arg)
                                        { return ipv6PrefixToHex(arg.column->getDataAt(row).toString(), result_type, context); }),
            std::back_inserter(ips),
            [](const std::string & arg) { return arg.size() != 0; });
    }

    else if (isArray(arguments.at(1).type))
    {
        DB::Field array0;
        arguments[1].column->get(row, array0);
        const auto len0 = array0.get<DB::Array>().size();

        for (size_t j = 0; j < len0; ++j)
        {
            if (const auto & value = array0.get<DB::Array>().at(j); value.getType() == DB::Field::Types::String)
            {
                const auto ipv6_string = ipv6PrefixToHex(toString(value), result_type, context);
                if (ipv6_string.size())
                {
                    ips.push_back(ipv6_string);
                }
            }
        }
    }
    return ips;
}

namespace DB
{
template <typename Name, ArgumentPolicy ap>
class FunctionKqlHasIpv6PrefixGeneric : public IFunction
{
public:
    static constexpr auto name = Name::name;
    static FunctionPtr create(ContextPtr context) { return std::make_shared<FunctionKqlHasIpv6PrefixGeneric>(std::move(context)); }

    explicit FunctionKqlHasIpv6PrefixGeneric(ContextPtr context_) : context(std::move(context_)) { }
    ~FunctionKqlHasIpv6PrefixGeneric() override = default;

    String getName() const override { return name; }
    size_t getNumberOfArguments() const override { return ap == ArgumentPolicy::Variadic ? 0 : 2; }
    bool isVariadic() const override { return ap == ArgumentPolicy::Variadic ? true : false; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        return getReturnType<ap>(arguments, getName());
    }

    ColumnPtr
    executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, const size_t input_rows_count) const override
    {
        auto result = ColumnUInt8::create();
        auto & result_column = result->getData();

        for (size_t i = 0; i < input_rows_count; ++i)
        {
            bool res = false;
            const auto ips = extractIpsFromArguments(arguments, result_type, context, i);

            std::string source = arguments[0].column->getDataAt(i).toString();
            const std::regex ip_finder("([^a-zA-Z0-9:.]|^)([0-9a-fA-F:.]{3,})([^a-zA-Z0-9:.]|$)");
            std::smatch matches;

            while (!res && std::regex_search(source, matches, ip_finder))
            {
                const auto m = ipv6ToHex(matches[2].str(), result_type, context);

                res = std::ranges::any_of(
                    ips,
                    [&m](const std::string & str) { return std::memcmp(str.c_str(), m.c_str(), std::min(str.size(), m.size())) == 0; });
                source = matches.suffix().str();
            }
            result_column.push_back(UInt8(res));
        }
        return result;
    }

private:
    ContextPtr context;
};

struct NameKqlHasAnyIpv6Prefix
{
    static constexpr auto name = "kql_has_any_ipv6_prefix";
};

struct NameKqlHasIpv6Prefix
{
    static constexpr auto name = "kql_has_ipv6_prefix";
};

using FunctionKqlHasAnyIpv6Prefix = FunctionKqlHasIpv6PrefixGeneric<NameKqlHasAnyIpv6Prefix, ArgumentPolicy::Variadic>;
using FunctionKqlHasIpv6Prefix = FunctionKqlHasIpv6PrefixGeneric<NameKqlHasIpv6Prefix, ArgumentPolicy::Single>;
REGISTER_FUNCTION(KqlHasIpv6PrefixGeneric)
{
    factory.registerFunction<FunctionKqlHasAnyIpv6Prefix>();
    factory.registerFunction<FunctionKqlHasIpv6Prefix>();
}
}
