#include <Core/Types.h>
#include <DataTypes/DataTypeString.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>
#include <Parsers/Kusto/KQLDataType.h>


namespace DB
{
class FunctionKqlGetType : public IFunction
{
public:
    static constexpr auto name = "kql_gettype";
    static FunctionPtr create(ContextPtr context) { return std::make_shared<FunctionKqlGetType>(std::move(context)); }

    explicit FunctionKqlGetType(ContextPtr context_) : context(std::move(context_)) { }
    ~FunctionKqlGetType() override = default;

    ColumnPtr
    executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override;
    String getName() const override { return name; }
    size_t getNumberOfArguments() const override { return 1; }
    DataTypePtr getReturnTypeImpl(const DataTypes &) const override { return std::make_shared<DataTypeString>(); }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }

private:
    ContextPtr context;
};

ColumnPtr
FunctionKqlGetType::executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, const size_t input_rows_count) const
{
    const auto & argument = arguments.front();
    return DataTypeString().createColumnConst(input_rows_count, toString(toKQLDataType(argument.type->getTypeId(), KQLScope::Row)));
}


REGISTER_FUNCTION(KqlGetType)
{
    factory.registerFunction<FunctionKqlGetType>();
}
}
