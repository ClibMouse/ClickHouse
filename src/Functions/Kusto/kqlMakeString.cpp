#include <Columns/ColumnString.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeString.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>

#include <codecvt>
#include <format>
#include <locale>

namespace DB
{
namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

class FunctionKqlMakeString : public IFunction
{
public:
    static constexpr auto name = "kql_make_string";
    static FunctionPtr create(ContextPtr context) { return std::make_shared<FunctionKqlMakeString>(std::move(context)); }

    explicit FunctionKqlMakeString(ContextPtr context_) : context(std::move(context_)) { }
    ~FunctionKqlMakeString() override = default;

    ColumnPtr
    executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override;
    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override;
    String getName() const override { return name; }
    bool isVariadic() const override { return true; }
    size_t getNumberOfArguments() const override { return 0; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }
    static String UnicodeToUTF8(const unsigned int & codepoint);

private:
    ContextPtr context;
};


String FunctionKqlMakeString::UnicodeToUTF8(const unsigned int & codepoint)
{
    String out;

    if (codepoint <= 0x7f)
        out.append(1, static_cast<char>(codepoint));
    else if (codepoint <= 0x7ff)
    {
        out.append(1, static_cast<char>(0xc0 | ((codepoint >> 6) & 0x1f)));
        out.append(1, static_cast<char>(0x80 | (codepoint & 0x3f)));
    }
    else if (codepoint <= 0xffff)
    {
        out.append(1, static_cast<char>(0xe0 | ((codepoint >> 12) & 0x0f)));
        out.append(1, static_cast<char>(0x80 | ((codepoint >> 6) & 0x3f)));
        out.append(1, static_cast<char>(0x80 | (codepoint & 0x3f)));
    }
    else
    {
        out.append(1, static_cast<char>(0xf0 | ((codepoint >> 18) & 0x07)));
        out.append(1, static_cast<char>(0x80 | ((codepoint >> 12) & 0x3f)));
        out.append(1, static_cast<char>(0x80 | ((codepoint >> 6) & 0x3f)));
        out.append(1, static_cast<char>(0x80 | (codepoint & 0x3f)));
    }
    return out;
}

DataTypePtr FunctionKqlMakeString::getReturnTypeImpl(const DataTypes & arguments) const
{
    if (const auto argument_count = std::ssize(arguments); argument_count < 1 || argument_count > 64)
        throw DB::Exception(
            DB::ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
            "Number of arguments for function {} doesn't match: passed {}, should be between 1 and 64",
            getName(),
            argument_count);

    const auto arg_it = std::ranges::find_if(arguments, [](const auto & argument) {
        if (const auto * array_type = typeid_cast<const DataTypeArray *>(argument.get()))
        {
            WhichDataType which(array_type->getNestedType()->getPtr());

            return !which.isUInt() && !which.isInt() && !which.isNothing();
        }
        return !WhichDataType(argument).isUInt() && !WhichDataType(argument).isInt();
    });

    if (arg_it != arguments.cend())
        throw DB::Exception(
            DB::ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
            "Arguments type argument # {} for function {} doesn't match: arguments should be integers int,long or a dynamic value holding "
            "an array of "
            "integral numbers",
            std::distance(arguments.cbegin(), arg_it),
            getName());

    return std::make_shared<DataTypeString>();
}

ColumnPtr
FunctionKqlMakeString::executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, const size_t input_rows_count) const
{
    auto out_col = ColumnString::create();
    for (size_t j = 0; j < input_rows_count; ++j)
    {
        String row_str;
        for (size_t i = 0; i < arguments.size(); ++i)
        {
            if (WhichDataType(arguments[i].type).isArray())
            {
                Field arr_field;
                arguments[i].column->get(j, arr_field);
                size_t len = arr_field.get<Array>().size();
                for (size_t k = 0; k < len; ++k)
                {
                    Field val = arr_field.get<Array>().at(k);
                    long int temp = val.get<Int64>();
                    if (temp < 0 || temp > 1114111)
                        throw DB::Exception(
                            DB::ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                            "Argument in function {} out of range, should be between 0 and 1114111",
                            getName());
                    row_str += UnicodeToUTF8(static_cast<unsigned int>(temp));
                }
            }
            else
            {
                long int temp = arguments[i].column->getInt(j);
                if (temp < 0 || temp > 1114111)
                    throw DB::Exception(
                        DB::ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                        "Argument in function {} out of range, should be between 0 and 1114111",
                        getName());
                row_str += UnicodeToUTF8(static_cast<unsigned int>(temp));
            }
        }
        out_col->insert(row_str);
    }
    return out_col;
}

REGISTER_FUNCTION(KqlMakeString)
{
    factory.registerFunction<FunctionKqlMakeString>();
}
}
