#include <DataTypes/DataTypeInterval.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>

namespace DB
{
class FunctionKqlToLong : public IFunction
{
public:
    static constexpr auto name = "kql_tolong";
    static FunctionPtr create(ContextPtr context) { return std::make_shared<FunctionKqlToLong>(std::move(context)); }

    explicit FunctionKqlToLong(ContextPtr context_) : context(std::move(context_)) { }
    ~FunctionKqlToLong() override = default;

    ColumnPtr
    executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override;
    String getName() const override { return name; }
    size_t getNumberOfArguments() const override { return 1; }
    DataTypePtr getReturnTypeImpl(const DataTypes &) const override
    {
        return std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt64>());
    }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }

private:
    ContextPtr context;
};

ColumnPtr FunctionKqlToLong::executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, const size_t input_rows_count) const
{
    const auto & argument = arguments.front();
    ColumnsWithTypeAndName firstarg{argument};
    WhichDataType which(*argument.type);
    if (which.isDateTime64())
    {
        const ColumnsWithTypeAndName todec_args{argument, createConstColumnWithTypeAndName<DataTypeUInt8>(9, "precision")};
        const auto todecimal128 = executeFunctionCall(context, "toDecimal128", todec_args, input_rows_count);
        //Seconds to microseconds
        const ColumnsWithTypeAndName multiplier_args{
            asArgument(todecimal128, name), createConstColumnWithTypeAndName<DataTypeInt64>(10000000, "multplier")};
        const auto multiplied = executeFunctionCall(context, "multiply", multiplier_args, input_rows_count);
        const ColumnsWithTypeAndName int_args{asArgument(multiplied, name)};
        const auto toint64 = executeFunctionCall(context, "toInt64", int_args, input_rows_count);
        //ClickHouse is unix epoch. KQL is from year 0. print tolong(datetime('1970-01-01'));
        const ColumnsWithTypeAndName plus_args{
            asArgument(toint64, name), createConstColumnWithTypeAndName<DataTypeInt64>(621355968000000000, "plus")};
        const auto plused = executeFunctionCall(context, "plus", plus_args, input_rows_count);
        firstarg = {asArgument(plused, name)};
    }
    const auto tostring = executeFunctionCall(context, "toString", firstarg, input_rows_count);
    ColumnsWithTypeAndName toint_args{asArgument(tostring, "tostring")};
    const auto out = executeFunctionCall(context, "toInt64OrNull", toint_args, input_rows_count);
    if (which.isInterval())
    {
        const ColumnsWithTypeAndName div_args{asArgument(out, "int64"), createConstColumnWithTypeAndName<DataTypeUInt8>(100, "intdiv")};
        return executeFunctionCall(context, "intDiv", div_args, input_rows_count).first;
    }
    return out.first;
}

REGISTER_FUNCTION(KqlToLong)
{
    factory.registerFunction<FunctionKqlToLong>();
}
}
