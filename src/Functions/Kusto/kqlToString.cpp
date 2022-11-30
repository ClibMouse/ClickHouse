#include <Columns/ColumnString.h>
#include <DataTypes/DataTypeString.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>
#include <Parsers/Kusto/ParserKQLTimespan.h>

#include <format>

namespace DB
{
class FunctionKqlToString : public IFunction
{
public:
    static constexpr auto name = "kql_tostring";
    static FunctionPtr create(ContextPtr context) { return std::make_shared<FunctionKqlToString>(std::move(context)); }

    explicit FunctionKqlToString(ContextPtr context_) : context(std::move(context_)) { }
    ~FunctionKqlToString() override = default;

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
FunctionKqlToString::executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, const size_t input_rows_count) const
{
    const auto & argument = arguments.front();
    if (WhichDataType(*argument.type).isInterval())
    {
        static const auto TICKS_PER_DAY = ParserKQLTimespan::parse("1d").value();
        static const auto TICKS_PER_HOUR = ParserKQLTimespan::parse("1h").value();
        static const auto TICKS_PER_MINUTE = ParserKQLTimespan::parse("1m").value();
        static const auto TICKS_PER_SECOND = ParserKQLTimespan::parse("1s").value();

        const auto & in_column = *argument.column;
        auto out_column = ColumnString::create();
        auto & chars = out_column->getChars();
        auto & offsets = out_column->getOffsets();
        for (size_t i = 0; i < input_rows_count; ++i)
        {
            const auto value = in_column.getInt(i);
            const auto abs_ticks = std::abs(value / 100);

            std::string timespan_as_string = value < 0 ? "-" : "";
            if (abs_ticks >= TICKS_PER_DAY)
                timespan_as_string.append(std::format("{}.", abs_ticks / TICKS_PER_DAY));

            timespan_as_string.append(std::format(
                "{:02}:{:02}:{:02}",
                (abs_ticks / TICKS_PER_HOUR) % 24,
                (abs_ticks / TICKS_PER_MINUTE) % 60,
                (abs_ticks / TICKS_PER_SECOND) % 60));

            if (const auto fractional_second = abs_ticks % TICKS_PER_SECOND)
                timespan_as_string.append(std::format(".{:07}", fractional_second));

            const auto chars_old_length = chars.size();
            const auto str_length_with_terminator = timespan_as_string.length() + 1;
            chars.resize(chars.size() + str_length_with_terminator);
            std::copy(timespan_as_string.c_str(), timespan_as_string.c_str() + str_length_with_terminator, chars.data() + chars_old_length);
            offsets.push_back(chars.size());
        }

        return out_column;
    }

    return executeFunctionCall(context, "toString", arguments, input_rows_count).first;
}

REGISTER_FUNCTION(KqlToString)
{
    factory.registerFunction<FunctionKqlToString>();
}
}
