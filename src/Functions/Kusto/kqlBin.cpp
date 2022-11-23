#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>

namespace
{
DB::ColumnWithTypeAndName
interpretAsInterval(const DB::ContextPtr & context, const DB::ColumnWithTypeAndName & argument, const size_t input_rows_count)
{
    static constexpr auto NANOSECONDS_PER_SECOND = 1'000'000'000U;

    const DB::ColumnsWithTypeAndName multiply_args{
        argument, DB::createConstColumnWithTypeAndName<DB::DataTypeUInt32>(NANOSECONDS_PER_SECOND, argument.name)};
    const auto product = executeFunctionCall(context, "multiply", multiply_args, input_rows_count);

    const DB::ColumnsWithTypeAndName to_interval_args{asArgument(product, argument.name)};
    const auto interval = executeFunctionCall(context, "toIntervalNanosecond", to_interval_args, input_rows_count);

    return asArgument(interval, argument.name);
}
}

namespace DB
{
class FunctionKqlBin : public IFunction
{
public:
    static constexpr auto name = "kql_bin";
    static FunctionPtr create(ContextPtr context) { return std::make_shared<FunctionKqlBin>(std::move(context)); }

    explicit FunctionKqlBin(ContextPtr context_) : context(std::move(context_)) { }
    ~FunctionKqlBin() override = default;

    ColumnPtr
    executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override;
    String getName() const override { return name; }
    size_t getNumberOfArguments() const override { return 2; }
    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override;
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }

private:
    ContextPtr context;
};

ColumnPtr
FunctionKqlBin::executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, const size_t input_rows_count) const
{
    const auto intermediate = std::invoke(
        [this, &arguments, &input_rows_count]
        {
            const auto & round_to_argument = arguments.back();
            const auto & value_argument = arguments.front();
            const WhichDataType round_to_which_data_type(*round_to_argument.type);
            const WhichDataType value_which_data_type(*value_argument.type);

            const auto & adjusted_round_to
                = (value_which_data_type.isDateTime64() || value_which_data_type.isInterval()) && !round_to_which_data_type.isInterval()
                ? interpretAsInterval(context, round_to_argument, input_rows_count)
                : round_to_argument;

            const ColumnsWithTypeAndName adjusted_args{value_argument, adjusted_round_to};
            if (value_which_data_type.isDateTime64())
                return executeFunctionCall(context, "toStartOfIntervalOrNull", adjusted_args, input_rows_count);

            const auto quotient = executeFunctionCall(context, "divide", adjusted_args, input_rows_count);

            const ColumnsWithTypeAndName floor_args{asArgument(quotient, adjusted_round_to.name)};
            const auto floored = executeFunctionCall(context, "floor", floor_args, input_rows_count);

            const ColumnsWithTypeAndName multiply_args{asArgument(floored, adjusted_round_to.name), adjusted_round_to};
            return executeFunctionCall(context, "multiply", multiply_args, input_rows_count);
        });

    const ColumnsWithTypeAndName conversion_args{
        asArgument(intermediate, "intermediate"), createConstColumnWithTypeAndName<DataTypeString>(result_type->getName(), "target_type")};
    return executeFunctionCall(context, "accurateCastOrNull", conversion_args, input_rows_count).first;
}

DataTypePtr FunctionKqlBin::getReturnTypeImpl(const DataTypes & arguments) const
{
    const auto & value_argument = arguments.front();
    const auto & round_to_argument = arguments.back();
    if (const WhichDataType value_which_data_type(*value_argument);
        value_which_data_type.isDateTime64() || value_which_data_type.isInterval() || isNumber(value_which_data_type))
    {
        const WhichDataType round_to_which_data_type(*round_to_argument);
        return makeNullable(
            isNumber(value_which_data_type) && (round_to_which_data_type.isFloat() || round_to_which_data_type.isDecimal())
                ? round_to_argument
                : value_argument);
    }

    throw Exception(
        ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
        "Illegal type {} of first argument of function {}, expected DateTime64, Interval or Number",
        value_argument->getName(),
        getName());
}

REGISTER_FUNCTION(KqlBin)
{
    factory.registerFunction<FunctionKqlBin>();
}
}
