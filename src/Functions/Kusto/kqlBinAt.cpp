#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>

namespace DB
{
class FunctionKqlBinAt : public IFunction
{
public:
    static constexpr auto name = "kql_bin_at";
    static FunctionPtr create(ContextPtr context) { return std::make_shared<FunctionKqlBinAt>(std::move(context)); }

    explicit FunctionKqlBinAt(ContextPtr context_) : context(std::move(context_)) { }
    ~FunctionKqlBinAt() override = default;

    ColumnPtr
    executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override;
    String getName() const override { return name; }
    size_t getNumberOfArguments() const override { return 3; }
    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override;
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }

private:
    ContextPtr context;
};

ColumnPtr FunctionKqlBinAt::executeImpl(
    const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, const size_t input_rows_count) const
{
    const auto get_or_convert_argument = [this, &input_rows_count](const ColumnWithTypeAndName & argument)
    {
        if (const WhichDataType which_data_type(*argument.type); which_data_type.isDateOrDate32() || which_data_type.isDateTime())
        {
            const ColumnsWithTypeAndName to_datetime64_args{
                argument,
                createConstColumnWithTypeAndName<DataTypeUInt8>(9, "scale"),
                createConstColumnWithTypeAndName<DataTypeString>("UTC", "timezone")};

            return asArgument(executeFunctionCall(context, "toDateTime64", to_datetime64_args, input_rows_count), argument.name);
        }

        return argument;
    };

    const auto & value_argument = get_or_convert_argument(arguments.front());
    const auto & round_to_argument = arguments[1];
    const auto & offset_argument = get_or_convert_argument(arguments.back());

    const ColumnsWithTypeAndName subtraction_args{value_argument, offset_argument};
    const auto difference = executeFunctionCall(context, "minus", subtraction_args, input_rows_count);

    const ColumnsWithTypeAndName bin_args{asArgument(difference, "difference"), round_to_argument};
    const auto bin_result = executeFunctionCall(context, "kql_bin", bin_args, input_rows_count);

    const ColumnsWithTypeAndName addition_args{offset_argument, asArgument(bin_result, "bin_result")};
    const auto sum = executeFunctionCall(context, "plus", addition_args, input_rows_count);

    const ColumnsWithTypeAndName cast_args{
        asArgument(sum, "sum"), createConstColumnWithTypeAndName<DataTypeString>(result_type->getName(), "type")};
    return executeFunctionCall(context, "cast", cast_args, input_rows_count).first;
}

DataTypePtr FunctionKqlBinAt::getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const
{
    const auto & value_type = *arguments.front().type;
    const auto & offset_type = *arguments.back().type;

    WhichDataType value_which_data_type(value_type);
    WhichDataType offset_which_data_type(offset_type);
    if ((value_which_data_type.isDateOrDate32OrDateTimeOrDateTime64() && offset_which_data_type.isDateOrDate32OrDateTimeOrDateTime64())
        || (value_which_data_type.isInterval() && offset_which_data_type.isInterval())
        || (isNumber(value_which_data_type) && isNumber(offset_which_data_type)))
    {
        const ColumnsWithTypeAndName bin_args{arguments.front(), arguments[1]};
        return FunctionFactory::instance().get("kql_bin", context)->build(bin_args)->getResultType();
    }

    throw Exception(
        ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
        "Illegal type {} of third argument of function {}, expected {}",
        offset_type.getName(),
        getName(),
        value_type.getFamilyName());
}

REGISTER_FUNCTION(KqlBinAt)
{
    factory.registerFunction<FunctionKqlBinAt>();
}
}
