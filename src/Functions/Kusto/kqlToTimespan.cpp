#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
#include <DataTypes/DataTypeInterval.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>
#include <Parsers/Kusto/ParserKQLTimespan.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

class FunctionKqlToTimespan : public IFunction
{
public:
    static constexpr auto name = "kql_totimespan";
    static FunctionPtr create(ContextPtr context) { return std::make_shared<FunctionKqlToTimespan>(std::move(context)); }

    explicit FunctionKqlToTimespan(ContextPtr context_) : context(std::move(context_)) { }
    ~FunctionKqlToTimespan() override = default;

    ColumnPtr
    executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override;
    String getName() const override { return name; }
    size_t getNumberOfArguments() const override { return 1; }
    DataTypePtr getReturnTypeImpl(const DataTypes &) const override;
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }
    bool useDefaultImplementationForConstants() const override { return true; }

private:
    ContextPtr context;
};

ColumnPtr FunctionKqlToTimespan::executeImpl(
    const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, const size_t input_rows_count) const
{
    const auto & argument = arguments.front();
    if (WhichDataType(*argument.type).isInterval())
        return wrapInNullable(argument.column, arguments, result_type, input_rows_count);

    const auto * in_column = typeid_cast<const ColumnString *>(argument.column.get());
    if (!in_column)
        throw Exception(
            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
            "Illegal type {} of first argument of function {}, expected String",
            argument.type->getName(),
            getName());

    auto out_column = result_type->createColumn();
    auto & out_column_as_nullable = assert_cast<ColumnNullable &>(*out_column);
    auto & out_nested_column = assert_cast<DataTypeInterval::ColumnType &>(out_column_as_nullable.getNestedColumn());

    const auto size = in_column->size();
    auto & out_data = out_nested_column.getData();
    auto & out_null_map = out_column_as_nullable.getNullMapData();
    out_data.resize(size);
    out_null_map.resize(size);

    const auto & in_chars = in_column->getChars();
    const auto & in_offsets = in_column->getOffsets();
    const auto * in_chars_data = reinterpret_cast<const char *>(in_chars.data());
    size_t start = 0;
    for (size_t i = 0; i < size; ++i)
    {
        const auto & offset = in_offsets[i];
        std::optional<Int64> ticks;
        const auto success = ParserKQLTimespan::tryParse({in_chars_data + start, offset - start - 1}, ticks);
        out_data[i] = ticks.value_or(0) * 100;
        out_null_map[i] = !ticks.has_value() || !success;

        start = offset;
    }

    return out_column;
}

DataTypePtr FunctionKqlToTimespan::getReturnTypeImpl(const DataTypes &) const
{
    return makeNullable(std::make_shared<DataTypeInterval>(IntervalKind::Nanosecond));
}

REGISTER_FUNCTION(KqlToTimespan)
{
    factory.registerFunction<FunctionKqlToTimespan>();
}
}
