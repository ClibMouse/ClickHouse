#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>
#include <Parsers/Kusto/ParserKQLTimespan.h>


#include <format>

namespace DB
{
namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

class FunctionKqlBetween : public IFunction
{
public:
    static constexpr auto name = "kql_between";
    static FunctionPtr create(ContextPtr context) { return std::make_shared<FunctionKqlBetween>(std::move(context)); }

    explicit FunctionKqlBetween(ContextPtr context_) : context(std::move(context_)) { }
    ~FunctionKqlBetween() override = default;

    ColumnPtr
    executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override;
    String getName() const override { return name; }
    size_t getNumberOfArguments() const override { return 3; }
    DataTypePtr getReturnTypeImpl(const DataTypes &) const override;
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }
    void checkTypeCompatibility(const DB::DataTypePtr arg1, const DB::DataTypePtr arg2) const;

private:
    ContextPtr context;
};

void FunctionKqlBetween::checkTypeCompatibility(const DB::DataTypePtr arg1, const DB::DataTypePtr arg2) const
{
    if (!arg1->getPtr()->equals(*arg2)
        && !(WhichDataType(arg1).isDateTime64() && (WhichDataType(arg2).isInterval() || WhichDataType(arg2).isDateTime64()))
        && !(
            (WhichDataType(arg1).isInt() || WhichDataType(arg1).isUInt() || WhichDataType(arg1).isFloat())
            && (WhichDataType(arg2).isInt() || WhichDataType(arg2).isUInt() || WhichDataType(arg2).isFloat())))
        throw DB::Exception(
            DB::ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
            "Arguments for function {} doesn't match: all arguments should be of same type",
            getName());
}

DataTypePtr FunctionKqlBetween::getReturnTypeImpl(const DataTypes & arguments) const
{
    checkTypeCompatibility(arguments[0], arguments[1]);
    checkTypeCompatibility(arguments[1], arguments[2]);

    const auto arg_it = std::ranges::find_if(arguments, [](const auto & argument) {
        return !WhichDataType(argument).isUInt() && !WhichDataType(argument).isInt() && !WhichDataType(argument).isFloat()
            && !WhichDataType(argument).isInterval() && !WhichDataType(argument).isDateTime64();
    });

    if (arg_it != arguments.cend())
        throw DB::Exception(
            DB::ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
            "Arguments type argument # {} for function {} doesn't match: arguments should be integer, long, real or datetime",
            std::distance(arguments.cbegin(), arg_it),
            getName());
    return DataTypeFactory::instance().get("Bool");
}

ColumnPtr
FunctionKqlBetween::executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, const size_t input_rows_count) const
{
    const auto & base_arg = arguments[0];
    const auto & comparable_arg1 = arguments[1];
    const auto & comparable_arg2 = arguments[2];

    const ColumnsWithTypeAndName lhs_and_value{comparable_arg1, base_arg};
    const auto lhs_and_value_compared = executeFunctionCall(context, "lessOrEquals", lhs_and_value, input_rows_count);

    if (!WhichDataType(*comparable_arg2.type).isInterval() || !WhichDataType(*comparable_arg1.type).isDateTime64())
    {
        const ColumnsWithTypeAndName value_and_rhs{base_arg, comparable_arg2};
        const auto value_and_rhs_compared = executeFunctionCall(context, "lessOrEquals", value_and_rhs, input_rows_count);
        const ColumnsWithTypeAndName comparisons{
            asArgument(lhs_and_value_compared, "lhs_and_value_compared"), asArgument(value_and_rhs_compared, "value_and_rhs_compared")};
        return executeFunctionCall(context, "and", comparisons, input_rows_count).first;
    }
    else
    {
        const ColumnsWithTypeAndName lhs_and_rhs{comparable_arg1, comparable_arg2};
        const auto lhs_and_rhs_sum = executeFunctionCall(context, "plus", lhs_and_rhs, input_rows_count);

        const ColumnsWithTypeAndName value_and_rhs{base_arg, asArgument(lhs_and_rhs_sum, "lhs_and_rhs_sum")};

        const auto value_and_rhs_compared = executeFunctionCall(context, "lessOrEquals", value_and_rhs, input_rows_count);
        const ColumnsWithTypeAndName comparisons{
            asArgument(lhs_and_value_compared, "lhs_and_value_compared"), asArgument(value_and_rhs_compared, "value_and_rhs_compared")};
        return executeFunctionCall(context, "and", comparisons, input_rows_count).first;
    }
}

REGISTER_FUNCTION(KqlBetween)
{
    factory.registerFunction<FunctionKqlBetween>();
}
}
