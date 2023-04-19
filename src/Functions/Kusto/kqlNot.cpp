#include <Columns/ColumnNullable.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/IDataType.h>
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

class FunctionKqlNot : public IFunction
{
public:
    static constexpr auto name = "kql_not";
    static FunctionPtr create(ContextPtr context) { return std::make_shared<FunctionKqlNot>(std::move(context)); }

    explicit FunctionKqlNot(ContextPtr context_) : context(std::move(context_)) { }
    ~FunctionKqlNot() override = default;

    ColumnPtr
    executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override;
    String getName() const override { return name; }
    size_t getNumberOfArguments() const override { return 1; }
    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override;
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }

private:
    ContextPtr context;
};

DataTypePtr FunctionKqlNot::getReturnTypeImpl(const DataTypes & arguments) const
{
    if (WhichDataType(*arguments[0]).isString() || WhichDataType(*arguments[0]).isArray())
        return makeNullable(std::make_shared<DataTypeString>());

    if (!WhichDataType(*arguments[0]).isInt() && !WhichDataType(*arguments[0]).isUInt())
        throw DB::Exception(
            DB::ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
            "Argument type for function {} doesn't match: argument expected to be a boolean, integer or dynamic expression",
            getName());

    return DataTypeFactory::instance().get("Bool");
}

ColumnPtr FunctionKqlNot::executeImpl(
    const ColumnsWithTypeAndName & arguments, const DataTypePtr & /*result_type*/, const size_t input_rows_count) const
{
    const auto & elem = arguments[0];

    WhichDataType arg_type(*elem.type);

    if (arg_type.isString() || arg_type.isArray())
        return makeNullable(DataTypeString().createColumnConst(input_rows_count, "NULL"));

    const ColumnsWithTypeAndName arg{arguments[0]};
    return executeFunctionCall(context, "not", arg, input_rows_count).first;
}

REGISTER_FUNCTION(KqlNot)
{
    factory.registerFunction<FunctionKqlNot>();
}
}
