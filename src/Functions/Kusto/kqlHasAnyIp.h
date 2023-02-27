#include <Columns/ColumnString.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnDecimal.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeString.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>
#include <Functions/Kusto/KqlFunctionBase.h>
#include <ranges>

namespace DB
{
namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int BAD_ARGUMENTS;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

enum ArgumentPolicy
{
    AP_Single,
    AP_Variadic
};

class KqlHasAnyIpArgument
{
public:
    static DataTypePtr ArgumentVerifier(const ColumnsWithTypeAndName & arguments, const std::string name, ArgumentPolicy ap)
    {
        const auto args_length = arguments.size();

        if (args_length < 2)
        {
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Number of arguments for function {} doesn't match: passed {}, should be 2 or more.", name, toString(arguments.size()));
        }

        if (!isStringOrFixedString(arguments.at(0).type))
        {
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Illegal type of argument of function {}", name);
        }

        if (!isStringOrFixedString(arguments.at(1).type) && !isArray(arguments.at(1).type))
        {
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Illegal type of argument of function {}", name);
        }

        if (isStringOrFixedString(arguments.at(1).type))
        {
            if (ap == AP_Variadic)
            {
                const auto are_arguments_valid = std::ranges::all_of(arguments | std::views::drop(2), [](const auto & argument) { return isStringOrFixedString(argument.type); });
                if (!are_arguments_valid)
                    throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Illegal type of argument of function {}", name);
            }
        }

        else if (ap == AP_Single || !isArray(arguments.at(1).type))
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Illegal type of argument of function {}", name);
        }

        return std::make_shared<DataTypeUInt8>();
    }
};
}
