#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>

#ifdef __clang__
#  pragma clang diagnostic push
#  pragma clang diagnostic ignored "-Wzero-as-null-pointer-constant"
#endif
#include <re2/re2.h>
#ifdef __clang__
#  pragma clang diagnostic pop
#endif

namespace DB::ErrorCodes
{
extern const int CANNOT_COMPILE_REGEXP;
extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

namespace DB
{
class FunctionKqlIndexOfRegex : public IFunction
{
public:
    static constexpr auto name = "kql_indexof_regex";
    static FunctionPtr create(ContextPtr context) { return std::make_shared<FunctionKqlIndexOfRegex>(std::move(context)); }

    explicit FunctionKqlIndexOfRegex(ContextPtr context_) : context(std::move(context_)) { }
    ~FunctionKqlIndexOfRegex() override = default;

    ColumnPtr
    executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override;
    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {1, 4}; }
    String getName() const override { return name; }
    size_t getNumberOfArguments() const override { return 0; }
    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override;
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }
    bool isVariadic() const override { return true; }

private:
    ColumnPtr extractArgumentColumnAsString(const ColumnWithTypeAndName & argument, const size_t input_rows_count) const
    {
        if (isString(argument.type))
            return argument.column;

        const ColumnsWithTypeAndName kql_to_string_args{argument};
        return executeFunctionCall(context, "kql_tostring", kql_to_string_args, input_rows_count).first;
    }

    ColumnPtr extractIntegerArgumentColumn(const ColumnsWithTypeAndName & arguments, const int index, const int default_value) const
    {
        if (index >= std::ssize(arguments))
            return DataTypeInt32().createColumnConst(1, toField(default_value));

        const auto & argument = arguments[index];
        if (!isInteger(argument.type))
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Illegal type {} of argument #{} of function {}, expected integral type",
                argument.type->getName(),
                index,
                getName());

        return argument.column;
    }

    ContextPtr context;
};

ColumnPtr
FunctionKqlIndexOfRegex::executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, const size_t input_rows_count) const
{
    const auto in_column_haystack = extractArgumentColumnAsString(arguments[0], input_rows_count);
    const auto in_column_pattern = extractArgumentColumnAsString(arguments[1], input_rows_count);
    const auto in_column_start = extractIntegerArgumentColumn(arguments, 2, 0);
    const auto in_column_length = extractIntegerArgumentColumn(arguments, 3, -1);
    const auto in_column_occurrence = extractIntegerArgumentColumn(arguments, 4, 1);

    const auto pattern = in_column_pattern->getDataAt(0).toView();
    const RE2 precompiled_pattern(pattern, RE2::Quiet);
    if (!precompiled_pattern.ok())
        throw Exception(ErrorCodes::CANNOT_COMPILE_REGEXP, "{}: {}", getName(), precompiled_pattern.error());

    auto out_column = ColumnInt64::create(input_rows_count);
    auto out_null_map = ColumnUInt8::create(input_rows_count);

    auto & out_column_data = out_column->getData();
    auto & out_null_map_data = out_null_map->getData();
    for (size_t i = 0; i < input_rows_count; ++i)
    {
        const auto start = in_column_start->getInt(i);
        const auto length = in_column_length->getInt(i);
        const auto occurrence = in_column_occurrence->getInt(i);

        const auto is_invalid = start < 0 || length < -1 || occurrence <= 0;
        out_null_map_data[i] = is_invalid;

        if (is_invalid)
            continue;

        const auto haystack = in_column_haystack->getDataAt(i).toView();
        const auto bounded_start = std::min(start, std::max(std::ssize(haystack) - 1, Int64(0)));
        const auto shortened_haystack = haystack.substr(bounded_start, length == -1 ? std::string_view::npos : length);

        size_t offset = 0;
        re2::StringPiece partial_match;
        int pass = 0;
        while (pass < occurrence
               && precompiled_pattern.Match(shortened_haystack, offset, shortened_haystack.length(), RE2::UNANCHORED, &partial_match, 1))
        {
            offset = std::distance(shortened_haystack.data(), partial_match.data()) + partial_match.length();
            ++pass;
        }

        out_column_data[i] = pass == occurrence ? std::distance(haystack.data(), partial_match.data()) : -1;
    }

    return ColumnNullable::create(std::move(out_column), std::move(out_null_map));
}

DataTypePtr FunctionKqlIndexOfRegex::getReturnTypeImpl(const DataTypes & arguments) const
{
    if (const auto argument_count = std::ssize(arguments); argument_count < 2 || 5 < argument_count)
        throw Exception(
            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
            "Number of arguments for function {} doesn't match: passed {}, should be between 2 and 5.",
            getName(),
            argument_count);

    return makeNullable(std::make_shared<DataTypeInt64>());
}

REGISTER_FUNCTION(KqlIndexOfRegex)
{
    factory.registerFunction<FunctionKqlIndexOfRegex>();
}
}
