#include <Columns/ColumnString.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnDecimal.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeString.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>
#include <Functions/Kusto/KqlFunctionBase.h>
#include <iostream>
#include <regex>

namespace DB
{
namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int BAD_ARGUMENTS;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}
template <typename Name, bool is_any>
class FunctionKqlHasIpv4PrefixGeneric : public IFunction
{
public:
    static constexpr auto name = Name::name;
    static FunctionPtr create(ContextPtr context) { return std::make_shared<FunctionKqlHasIpv4PrefixGeneric>(std::move(context)); }

    explicit FunctionKqlHasIpv4PrefixGeneric(ContextPtr context_) : context(std::move(context_)) { }
    ~FunctionKqlHasIpv4PrefixGeneric() override = default;

    String getName() const override { return name; }
    size_t getNumberOfArguments() const override { return is_any ? 0 : 2; }
    bool isVariadic() const override { return is_any ? true : false; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        const auto args_length = arguments.size();

        if (args_length < 2)
        {
            throw Exception(
                "Number of arguments for function " + getName() + " doesn't match: passed " + toString(arguments.size())
                + ", should be 2 or more.",
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);
        }

        if (!isStringOrFixedString(arguments.at(0).type))
        {
            throw Exception("Illegal type of argument of function " + getName(), ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
        }

        if (!isStringOrFixedString(arguments.at(1).type) && !isArray(arguments.at(1).type))
        {
            throw Exception("Illegal type of argument of function " + getName(), ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
        }

        if (isStringOrFixedString(arguments.at(1).type))
        {
            if (is_any)
            {
                for (size_t i = 2; i < args_length; i++)
                {
                    if (!isStringOrFixedString(arguments.at(i).type))
                    {
                        throw Exception("Illegal type of argument of function " + getName(), ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
                    }
                }
            }
        }

        else if (is_any == false || !isArray(arguments.at(1).type))
        {
            throw Exception("Illegal type of argument of function " + getName(), ErrorCodes::BAD_ARGUMENTS);
        }

        return std::make_shared<DataTypeUInt8>();
    }

    ColumnPtr executeImpl(
        const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, const size_t input_rows_count) const override
    {
        const auto args_length = arguments.size();
        std::vector<std::string> ips;
        auto bool_type = std::make_shared<DataTypeUInt8>();

        if (input_rows_count && isStringOrFixedString(arguments.at(1).type))
        {
            for (size_t i = 1; i < args_length; i++)
            {
                std::string ip_prefix = arguments[i].column->getDataAt(0).toString();

                if (ip_prefix.empty())
                {
                    continue;
                }

                if (std::count(ip_prefix.begin(), ip_prefix.end(), '.') == 3 || ip_prefix.back() == '.')
                {
                    ips.push_back(ip_prefix);
                }
            }
        }

        else if (input_rows_count && isArray(arguments.at(1).type))
        {
            Field array0;
            arguments[1].column->get(0, array0);
            size_t len0 = array0.get<Array>().size();
            for (size_t k = 0; k < len0; k++)
            {
                if (array0.get<Array>().at(k).getType() == Field::Types::String)
                {
                    std::string ip_prefix = toString(array0.get<Array>().at(k));

                    if (ip_prefix.empty())
                    {
                        continue;
                    }

                    if (std::count(ip_prefix.begin(), ip_prefix.end(), '.') == 3 || ip_prefix.back() == '.')
                    {
                        ips.push_back(ip_prefix);
                    }
                }
            }
        }

        if (!ips.empty())
        {
            std::string source = arguments[0].column->getDataAt(0).toString();
            std::regex ip_finder("([^[:alnum:]]|^)([0-9]{1,3}\\.[0-9]{1,3}\\.[0-9]{1,3}\\.[0-9]{1,3})([^[:alnum:]]|$)");
            std::smatch matches;

            if (std::regex_search(source, matches, ip_finder))
            {
                for (size_t i = 0; i < matches.size(); i++)
                {
                    ColumnPtr column_ip = DataTypeString().createColumnConst(1, toField(String(matches[i])));
                    const ColumnsWithTypeAndName isipv4string_args = {ColumnWithTypeAndName(column_ip, std::make_shared<DataTypeString>(), "ip")};

                    auto isipv4 = FunctionFactory::instance()
                        .get("isIPv4String", context)
                        ->build(isipv4string_args)
                        ->execute(isipv4string_args, result_type, input_rows_count);

                    if (isipv4->getUInt(0) == 1)
                    {
                        if (std::any_of(ips.begin(), ips.end(), [i, matches](const std::string & str) -> bool { return str == matches[i].str().substr(0, str.size()); }))
                        {
                            return bool_type->createColumnConst(input_rows_count,1);
                        }

                    }

                }
            }
        }

        return bool_type->createColumnConst(input_rows_count,0);
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

using FunctionKqlHasAnyIpv4Prefix = FunctionKqlHasIpv4PrefixGeneric<NameKqlHasAnyIpv4Prefix, true>;
using FunctionKqlHasIpv4Prefix    = FunctionKqlHasIpv4PrefixGeneric<NameKqlHasIpv4Prefix, false>;

REGISTER_FUNCTION(KqlHasIpv4PrefixGeneric)
{
    factory.registerFunction<FunctionKqlHasAnyIpv4Prefix>();
    factory.registerFunction<FunctionKqlHasIpv4Prefix>();
}
}
