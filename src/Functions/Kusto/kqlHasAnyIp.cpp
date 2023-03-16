#include <ranges>
#include <regex>
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

namespace DB
{
template <typename Name, ArgumentPolicy ap, SearchType search>
class FunctionKqlHasIpGeneric : public IFunction
{
public:
    static constexpr auto name = Name::name;
    static FunctionPtr create(ContextPtr context) { return std::make_shared<FunctionKqlHasIpGeneric>(std::move(context)); }

    explicit FunctionKqlHasIpGeneric(ContextPtr context_) : context(std::move(context_)) { }
    ~FunctionKqlHasIpGeneric() override = default;

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
            const auto ips = extractIpsFromArguments(arguments, result_type, context, i, search);

            std::string source = arguments[0].column->getDataAt(i).toString();
            const std::regex ip_finder(
                (search == SearchType::IPv4 || search == SearchType::IPv4_Prefix)
                    ? "([^[:alnum:]]|^)([0-9]{1,3}\\.[0-9]{1,3}\\.[0-9]{1,3}\\.[0-9]{1,3})([^[:alnum:]]|$)"
                    : "([^a-zA-Z0-9:.]|^)([0-9a-fA-F:.]{3,})([^a-zA-Z0-9:.]|$)");
            std::smatch matches;

            while (!res && std::regex_search(source, matches, ip_finder))
            {
                if (search == SearchType::IPv4 || search == SearchType::IPv4_Prefix)
                {
                    const auto match_as_str = matches[2].str();

                    const ColumnsWithTypeAndName is_ipv4_string_args
                        = {createConstColumnWithTypeAndName<DataTypeString>(match_as_str, "ip")};

                    const auto is_ipv4 = FunctionFactory::instance()
                                             .get("isIPv4String", context)
                                             ->build(is_ipv4_string_args)
                                             ->execute(is_ipv4_string_args, result_type, 1);

                    if (is_ipv4->getUInt(0) == 1)
                    {
                        if (search == SearchType::IPv4)
                        {
                            res = std::ranges::any_of(ips, std::bind_front(std::equal_to<std::string>(), match_as_str));
                        }
                        else
                        {
                            res = std::ranges::any_of(
                                ips,
                                [&match_as_str](const std::string & str) -> bool
                                { return std::memcmp(str.c_str(), match_as_str.c_str(), std::min(str.size(), match_as_str.size())) == 0; });
                        }
                    }
                }
                else
                {
                    const auto m = ipv6ToHex(matches[2].str(), result_type, context);

                    if (search == SearchType::IPv6)
                    {
                        res = std::ranges::any_of(ips, std::bind_front(std::equal_to<std::string>(), m));
                    }
                    else
                    {
                        res = std::ranges::any_of(
                        ips,
                        [&m](const std::string & str) { return std::memcmp(str.c_str(), m.c_str(), std::min(str.size(), m.size())) == 0; });
                    }
                }

                source = matches.suffix().str();
            }
            result_column.push_back(static_cast<UInt8>(res));
        }
        return result;
    }

private:
    ContextPtr context;
};

struct NameKqlHasAnyIpv4
{
    static constexpr auto name = "kql_has_any_ipv4";
};

struct NameKqlHasIpv4
{
    static constexpr auto name = "kql_has_ipv4";
};

struct NameKqlHasAnyIpv4Prefix
{
    static constexpr auto name = "kql_has_any_ipv4_prefix";
};

struct NameKqlHasIpv4Prefix
{
    static constexpr auto name = "kql_has_ipv4_prefix";
};

struct NameKqlHasAnyIpv6
{
    static constexpr auto name = "kql_has_any_ipv6";
};

struct NameKqlHasIpv6
{
    static constexpr auto name = "kql_has_ipv6";
};

struct NameKqlHasAnyIpv6Prefix
{
    static constexpr auto name = "kql_has_any_ipv6_prefix";
};

struct NameKqlHasIpv6Prefix
{
    static constexpr auto name = "kql_has_ipv6_prefix";
};

using FunctionKqlHasAnyIpv4 = FunctionKqlHasIpGeneric<NameKqlHasAnyIpv4, ArgumentPolicy::Variadic, SearchType::IPv4>;
using FunctionKqlHasIpv4 = FunctionKqlHasIpGeneric<NameKqlHasIpv4, ArgumentPolicy::Single, SearchType::IPv4>;
using FunctionKqlHasAnyIpv4Prefix
    = FunctionKqlHasIpGeneric<NameKqlHasAnyIpv4Prefix, ArgumentPolicy::Variadic, SearchType::IPv4_Prefix>;
using FunctionKqlHasIpv4Prefix = FunctionKqlHasIpGeneric<NameKqlHasIpv4Prefix, ArgumentPolicy::Single, SearchType::IPv4_Prefix>;
using FunctionKqlHasAnyIpv6 = FunctionKqlHasIpGeneric<NameKqlHasAnyIpv6, ArgumentPolicy::Variadic, SearchType::IPv6>;
using FunctionKqlHasIpv6 = FunctionKqlHasIpGeneric<NameKqlHasIpv6, ArgumentPolicy::Single, SearchType::IPv6>;
using FunctionKqlHasAnyIpv6Prefix
    = FunctionKqlHasIpGeneric<NameKqlHasAnyIpv6Prefix, ArgumentPolicy::Variadic, SearchType::IPv6_Prefix>;
using FunctionKqlHasIpv6Prefix = FunctionKqlHasIpGeneric<NameKqlHasIpv6Prefix, ArgumentPolicy::Single, SearchType::IPv6_Prefix>;

REGISTER_FUNCTION(FunctionKqlHasIpGeneric)
{
    factory.registerFunction<FunctionKqlHasAnyIpv4>();
    factory.registerFunction<FunctionKqlHasIpv4>();
    factory.registerFunction<FunctionKqlHasAnyIpv4Prefix>();
    factory.registerFunction<FunctionKqlHasIpv4Prefix>();
    factory.registerFunction<FunctionKqlHasAnyIpv6>();
    factory.registerFunction<FunctionKqlHasIpv6>();
    factory.registerFunction<FunctionKqlHasAnyIpv6Prefix>();
    factory.registerFunction<FunctionKqlHasIpv6Prefix>();
}
}
