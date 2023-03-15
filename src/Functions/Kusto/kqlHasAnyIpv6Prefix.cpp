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
            const auto ips = extractIpsFromArguments(arguments, result_type, context, i, TransformType::IPv6_Prefix);

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
