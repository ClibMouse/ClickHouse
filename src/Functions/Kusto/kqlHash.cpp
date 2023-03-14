#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>
#include <Parsers/Kusto/ParserKQLTimespan.h>

#include <format>

namespace DB
{
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
    size_t getNumberOfArguments() const override { return 1; }
    DataTypePtr getReturnTypeImpl(const DataTypes &) const override { return std::make_shared<DataTypeInt64>(); }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }

private:
    ContextPtr context;
};

ColumnPtr FunctionKqlHash::executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, const size_t input_rows_count) const
{
    const auto & argument = arguments.front();
    WhichDataType which(*argument.type);
    ColumnsWithTypeAndName args = arguments;
    /* Int32 left out for the time being so that int KQL explicit int cast will still function 
     * ClickHouse will put nubmers in the smallest data type they will fit it.
     * Numbers not cast interpreted as Int32 by ClickHouse will not match KQL (int64) results 
    */
    if (which.isInt8() || which.isInt16() || which.isUInt8() || which.isUInt32() || which.isUInt64())
    {
        const auto tocast = executeFunctionCall(context, "toInt64", arguments, input_rows_count);
        args = {asArgument(tocast, name)};
    }
    else if (which.isFloat32())
    {
        const auto tocast = executeFunctionCall(context, "toFloat64", arguments, input_rows_count);
        args = {asArgument(tocast, name)};
    }
    const auto tohash = executeFunctionCall(context, "xxHash64", args, input_rows_count);
    const ColumnsWithTypeAndName hashargs{asArgument(tohash, "tohash")};
    return executeFunctionCall(context, "toInt64", hashargs, input_rows_count).first;
}

REGISTER_FUNCTION(KqlHash)
{
    factory.registerFunction<FunctionKqlHash>();
}
}
