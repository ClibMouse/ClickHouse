#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

class FunctionKqlHash : public IFunction
{
public:
    static constexpr auto name = "kql_hash";
    static FunctionPtr create(ContextPtr context) { return std::make_shared<FunctionKqlHash>(std::move(context)); }

    explicit FunctionKqlHash(ContextPtr context_) : context(std::move(context_)) { }
    ~FunctionKqlHash() override = default;

    ColumnPtr
    executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override;
    String getName() const override { return name; }
    size_t getNumberOfArguments() const override { return 0; }
    bool isVariadic() const override { return true; }
    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override;
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }

private:
    ContextPtr context;
};

ColumnPtr FunctionKqlHash::executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, const size_t input_rows_count) const
{
    const auto argument = arguments.front();
    const ColumnsWithTypeAndName firstarg{argument};
    WhichDataType which(*argument.type);
    ColumnsWithTypeAndName args = firstarg;
    /* Int32 left out for the time being so that int KQL explicit int cast will still function
     * ClickHouse will put nubmers in the smallest data type they will fit it.
     * Numbers not cast interpreted as Int32 by ClickHouse will not match KQL (int64) results
    */
    if (which.isInt8() || which.isInt16() || which.isUInt8() || which.isUInt16() || which.isUInt32() || which.isUInt64())
    {
        const auto tocast = executeFunctionCall(context, "toInt64", firstarg, input_rows_count);
        args = {asArgument(tocast, name)};
    }
    else if (which.isFloat32())
    {
        const auto tocast = executeFunctionCall(context, "toFloat64", firstarg, input_rows_count);
        args = {asArgument(tocast, name)};
    }
    const auto tohash = executeFunctionCall(context, "xxHash64", args, input_rows_count);
    const ColumnsWithTypeAndName hashargs{asArgument(tohash, "tohash")};
    if (arguments.size() == 1)
        return executeFunctionCall(context, "toInt64", hashargs, input_rows_count).first;
    else
    {
        const ColumnsWithTypeAndName modargs{asArgument(tohash, "tohash"), arguments.back()};
        const auto tomod = executeFunctionCall(context, "moduloOrZero", modargs, input_rows_count);
        const ColumnsWithTypeAndName touint{asArgument(tomod, "tomod")};
        return executeFunctionCall(context, "toInt64", touint, input_rows_count).first;
    }
}

DataTypePtr FunctionKqlHash::getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const
{
    if (arguments.size() == 0 || arguments.size() > 2)
        throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "hash(): function expects [1..2] argument(s).");

    if (arguments.size() == 2 && !isUnsignedInteger(arguments[1].type))
    {
        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "hash(): argument 2 must be a constant positive long value");
    }
    return std::make_shared<DataTypeInt64>();
}

REGISTER_FUNCTION(KqlHash)
{
    factory.registerFunction<FunctionKqlHash>();
}
}
