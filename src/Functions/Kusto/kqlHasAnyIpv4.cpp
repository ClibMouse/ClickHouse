#include <Columns/ColumnString.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnDecimal.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeString.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>
#include <Functions/Kusto/KqlFunctionBase.h>
#include <regex>
#include <ranges>

namespace DB
{
namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int BAD_ARGUMENTS;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

template <typename Name, bool is_any>
class FunctionKqlHasIpv4Generic : public IFunction
{
public:
    static constexpr auto name = Name::name;
    static FunctionPtr create(ContextPtr context) { return std::make_shared<FunctionKqlHasIpv4Generic>(std::move(context)); }

    explicit FunctionKqlHasIpv4Generic(ContextPtr context_) : context(std::move(context_)) { }
    ~FunctionKqlHasIpv4Generic() override = default;

    String getName() const override { return name; }
    size_t getNumberOfArguments() const override { return is_any ? 0 : 2; }
    bool isVariadic() const override { return is_any ? true : false; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        const auto args_length = arguments.size();

        if (args_length < 2)
        {
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Number of arguments for function {} doesn't match: passed {}, should be 2 or more.", getName(), toString(arguments.size()));
        }

        if (!isStringOrFixedString(arguments.at(0).type))
        {
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Illegal type of argument of function {}", getName());
        }

        if (!isStringOrFixedString(arguments.at(1).type) && !isArray(arguments.at(1).type))
        {
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Illegal type of argument of function {}", getName());
        }

        if (isStringOrFixedString(arguments.at(1).type))
        {
            if constexpr (is_any)
            {
                const auto are_arguments_valid = std::ranges::all_of(arguments | std::views::drop(2), [](const auto & argument) { return isStringOrFixedString(argument.type); });
                if (!are_arguments_valid)
                    throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Illegal type of argument of function {}", getName());
            }
        }

        else if (!is_any || !isArray(arguments.at(1).type))
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Illegal type of argument of function {}", getName());
        }

        return std::make_shared<DataTypeUInt8>();
    }

    ColumnPtr executeImpl(
        const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, const size_t input_rows_count) const override
    {
        const auto args_length = arguments.size();
        auto result_column = ColumnUInt8::create();
        auto isipv4string = [&, result_type] (ColumnsWithTypeAndName args) { return FunctionFactory::instance().get("isIPv4String", context)->build(args)->execute(args, result_type, 1); };

        for (size_t i = 0; i < input_rows_count; i++)
        {
            bool res = false;
            std::vector<std::string> ips;
            if (isStringOrFixedString(arguments.at(1).type))
            {
                for (size_t j = 1; j < args_length; j++)
                {
                    std::string arg = arguments[j].column->size() == input_rows_count ? arguments[j].column->getDataAt(i).toString() : arguments[j].column->getDataAt(0).toString();

                    ColumnPtr column_ip = DataTypeString().createColumnConst(1, toField(String(arg)));
                    const ColumnsWithTypeAndName isipv4string_args = {ColumnWithTypeAndName(column_ip, std::make_shared<DataTypeString>(), "ip")};

                    auto isipv4 = isipv4string(isipv4string_args);
                    if (isipv4->getUInt(0) == 1)
                    {
                        ips.push_back(arg);
                    }
                }
            }

            else if (isArray(arguments.at(1).type))
            {
                Field array0;
                arguments[1].column->size() == input_rows_count ? arguments[1].column->get(i, array0) : arguments[1].column->get(0, array0);
                const auto len0 = array0.get<Array>().size();

                for (size_t j = 0; j < len0; j++)
                {
                    if (array0.get<Array>().at(j).getType() == Field::Types::String)
                    {
                        ColumnPtr column_ip = DataTypeString().createColumnConst(1, array0.get<Array>().at(j));
                        const ColumnsWithTypeAndName isipv4string_args = {ColumnWithTypeAndName(column_ip, std::make_shared<DataTypeString>(), "ip")};

                        auto isipv4 = isipv4string(isipv4string_args);
                        if (isipv4->getUInt(0) == 1)
                        {
                            ips.push_back(toString(array0.get<Array>().at(j)));
                        }
                    }
                }
            }

            if (!ips.empty())
            {
                std::string source = arguments[0].column->size() == input_rows_count ? arguments[0].column->getDataAt(i).toString() : arguments[0].column->getDataAt(0).toString();
                std::regex ip_finder("([^[:alnum:]]|^)([0-9]{1,3}\\.[0-9]{1,3}\\.[0-9]{1,3}\\.[0-9]{1,3})([^[:alnum:]]|$)");
                std::smatch matches;

                while (std::regex_search(source, matches, ip_finder))
                {
                    for (size_t j = 0; j < matches.size(); j++)
                    {
                        if (std::any_of(ips.begin(), ips.end(), [j, matches](const std::string & str) -> bool { return str == matches[j]; }))
                        {
                            res = true;
                            break;
                        }
                    }
                    source = matches.suffix().str();
                }
            }
            result_column->insertValue(UInt8(res));
        }
        return result_column;
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

using FunctionKqlHasAnyIpv4 = FunctionKqlHasIpv4Generic<NameKqlHasAnyIpv4, true>;
using FunctionKqlHasIpv4    = FunctionKqlHasIpv4Generic<NameKqlHasIpv4, false>;

REGISTER_FUNCTION(KqlHasIpv4Generic)
{
    factory.registerFunction<FunctionKqlHasAnyIpv4>();
    factory.registerFunction<FunctionKqlHasIpv4>();
}
}
