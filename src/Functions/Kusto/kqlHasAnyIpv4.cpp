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

static std::vector<std::string> extractIpsFromArguments(
    const DB::ColumnsWithTypeAndName & arguments, const DB::DataTypePtr & result_type, const DB::ContextPtr & context, size_t row)
{
    std::vector<std::string> ips;
    const auto is_ipv4_string = [&, result_type](const DB::ColumnsWithTypeAndName & args)
    { return DB::FunctionFactory::instance().get("isIPv4String", context)->build(args)->execute(args, result_type, 1); };
    if (DB::isStringOrFixedString(arguments.at(1).type))
    {
        std::ranges::copy_if(
            arguments | std::views::drop(1) | std::views::transform( [&row](const DB::ColumnWithTypeAndName & arg) { return arg.column->getDataAt(row).toString(); }),
            std::back_inserter(ips),
            [& is_ipv4_string](const std::string & arg)
            {
                const DB::ColumnsWithTypeAndName is_ipv4_string_args
                    = {DB::createConstColumnWithTypeAndName<DB::DataTypeString>(arg, "ip")};
                const auto is_ipv4 = is_ipv4_string(is_ipv4_string_args);
                return is_ipv4->getUInt(0);
            });
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
                const auto value_as_string = toString(value);
                const DB::ColumnsWithTypeAndName is_ipv4_string_args
                    = {DB::createConstColumnWithTypeAndName<DB::DataTypeString>(value_as_string, "ip")};
                const auto is_ipv4 = is_ipv4_string(is_ipv4_string_args);
                if (is_ipv4->getUInt(0) == 1)
                {
                    ips.push_back(value_as_string);
                }
            }
        }
    }
    return ips;
}

namespace DB
{
template <typename Name, ArgumentPolicy ap>
class FunctionKqlHasIpv4Generic : public IFunction
{
public:
    static constexpr auto name = Name::name;
    static FunctionPtr create(ContextPtr context) { return std::make_shared<FunctionKqlHasIpv4Generic>(std::move(context)); }

    explicit FunctionKqlHasIpv4Generic(ContextPtr context_) : context(std::move(context_)) { }
    ~FunctionKqlHasIpv4Generic() override = default;

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
            const std::regex ip_finder("([^[:alnum:]]|^)([0-9]{1,3}\\.[0-9]{1,3}\\.[0-9]{1,3}\\.[0-9]{1,3})([^[:alnum:]]|$)");
            std::smatch matches;

            while (!res && std::regex_search(source, matches, ip_finder))
            {
                res = std::ranges::any_of(
                    matches,
                    [&ips](const std::ssub_match & m)
                    { return std::ranges::any_of(ips, std::bind_front(std::equal_to<std::string>(), m)); });

                source = matches.suffix().str();
            }
            result_column.push_back(UInt8(res));
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

using FunctionKqlHasAnyIpv4 = FunctionKqlHasIpv4Generic<NameKqlHasAnyIpv4, ArgumentPolicy::Variadic>;
using FunctionKqlHasIpv4 = FunctionKqlHasIpv4Generic<NameKqlHasIpv4, ArgumentPolicy::Single>;
REGISTER_FUNCTION(KqlHasIpv4Generic)
{
    factory.registerFunction<FunctionKqlHasAnyIpv4>();
    factory.registerFunction<FunctionKqlHasIpv4>();
}
}
