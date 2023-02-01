#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeInterval.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>

#include <format>

namespace DB::ErrorCodes
{
extern const int LOGICAL_ERROR;
}

namespace
{
enum class InputPolicy
{
    Arbitrary,
    Constant
};

constexpr const char * getDateTimeParsingFunction(const InputPolicy input_policy)
{
    if (input_policy == InputPolicy::Arbitrary)
        return "parseDateTime64BestEffortOrNull";
    else if (input_policy == InputPolicy::Constant)
        return "parseDateTime64BestEffort";

    throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Unhandled input policy: {}", magic_enum::enum_name(input_policy));
}

constexpr const char * getFunctionName(const InputPolicy input_policy)
{
    if (input_policy == InputPolicy::Arbitrary)
        return "kql_todatetime";
    else if (input_policy == InputPolicy::Constant)
        return "kql_datetime";

    throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Unhandled input policy: {}", magic_enum::enum_name(input_policy));
}
}

namespace DB
{
template <InputPolicy input_policy>
class FunctionKqlDateTime : public IFunction
{
public:
    static constexpr auto name = getFunctionName(input_policy);
    static FunctionPtr create(ContextPtr context) { return std::make_shared<FunctionKqlDateTime>(std::move(context)); }

    explicit FunctionKqlDateTime(ContextPtr context_) : context(std::move(context_)) { }
    ~FunctionKqlDateTime() override = default;

    ColumnPtr
    executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override;
    String getName() const override { return name; }
    size_t getNumberOfArguments() const override { return 1; }
    DataTypePtr getReturnTypeImpl(const DataTypes &) const override { return makeNullable(std::make_shared<DataTypeDateTime64>(9, "UTC")); }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }

private:
    ContextPtr context;
};

template <InputPolicy input_policy>
ColumnPtr FunctionKqlDateTime<input_policy>::executeImpl(
    const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, const size_t input_rows_count) const
{
    const auto & argument = arguments.front();
    const ColumnsWithTypeAndName conversion_args{
        argument,
        createConstColumnWithTypeAndName<DataTypeUInt8>(9, "scale"),
        createConstColumnWithTypeAndName<DataTypeString>("UTC", "timezone")};

    const auto * const conversion_function
        = WhichDataType(*argument.type).isStringOrFixedString() ? getDateTimeParsingFunction(input_policy) : "toDateTime64";
    const auto converted = executeFunctionCall(context, conversion_function, conversion_args, input_rows_count);

    const ColumnsWithTypeAndName addition_args{
        asArgument(converted, "converted"),
        createConstColumnWithTypeAndName<DataTypeInterval>(50, "interval_50", IntervalKind::Nanosecond)};
    const auto sum = executeFunctionCall(context, "plus", addition_args, input_rows_count);

    const ColumnsWithTypeAndName to_start_of_interval_args{
        asArgument(sum, "sum"), createConstColumnWithTypeAndName<DataTypeInterval>(100, "interval_100", IntervalKind::Nanosecond)};
    const auto [rounded_column, _] = executeFunctionCall(context, "toStartOfInterval", to_start_of_interval_args, input_rows_count);

    return wrapInNullable(rounded_column, conversion_args, result_type, input_rows_count);
}

REGISTER_FUNCTION(KqlDateTime)
{
    factory.registerFunction<FunctionKqlDateTime<InputPolicy::Arbitrary>>();
    factory.registerFunction<FunctionKqlDateTime<InputPolicy::Constant>>();
}
}
