#include <cstring>
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
template <typename Name, ArgumentPolicy ap>
class FunctionKqlHasIpv4PrefixGeneric : public IFunction
{
public:
    static constexpr auto name = Name::name;
    static FunctionPtr create(ContextPtr context) { return std::make_shared<FunctionKqlHasIpv4PrefixGeneric>(std::move(context)); }

    explicit FunctionKqlHasIpv4PrefixGeneric(ContextPtr context_) : context(std::move(context_)) { }
    ~FunctionKqlHasIpv4PrefixGeneric() override = default;

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
            const auto ips = extractIpsFromArguments(arguments, result_type, context, i, TransformType::IPv4_Prefix);

            std::string source = arguments[0].column->getDataAt(i).toString();
            const std::regex ip_finder("([^[:alnum:]]|^)([0-9]{1,3}\\.[0-9]{1,3}\\.[0-9]{1,3}\\.[0-9]{1,3})([^[:alnum:]]|$)");
            std::smatch matches;

            while (!res && std::regex_search(source, matches, ip_finder))
            {
                const auto match_as_str = matches[2].str();

                const ColumnsWithTypeAndName is_ipv4_string_args = {createConstColumnWithTypeAndName<DataTypeString>(match_as_str, "ip")};

                const auto is_ipv4 = FunctionFactory::instance()
                                         .get("isIPv4String", context)
                                         ->build(is_ipv4_string_args)
                                         ->execute(is_ipv4_string_args, result_type, 1);

                if (is_ipv4->getUInt(0) == 1)
                {
                    res = std::ranges::any_of(
                        ips,
                        [&match_as_str](const std::string & str) -> bool
                        { return std::memcmp(str.c_str(), match_as_str.c_str(), std::min(str.size(), match_as_str.size())) == 0; });
                }
                source = matches.suffix().str();
            }
            result_column.push_back(UInt8(res));
        }

        return result;
    }

private:
    ContextPtr context;
};

struct NameKqlHasAnyIpv4Prefix
{
    static constexpr auto name = "kql_has_any_ipv4_prefix";
};

struct NameKqlHasIpv4Prefix
{
    static constexpr auto name = "kql_has_ipv4_prefix";
};

using FunctionKqlHasAnyIpv4Prefix = FunctionKqlHasIpv4PrefixGeneric<NameKqlHasAnyIpv4Prefix, ArgumentPolicy::Variadic>;
using FunctionKqlHasIpv4Prefix = FunctionKqlHasIpv4PrefixGeneric<NameKqlHasIpv4Prefix, ArgumentPolicy::Single>;

REGISTER_FUNCTION(KqlHasIpv4PrefixGeneric)
{
    factory.registerFunction<FunctionKqlHasAnyIpv4Prefix>();
    factory.registerFunction<FunctionKqlHasIpv4Prefix>();
}
}
