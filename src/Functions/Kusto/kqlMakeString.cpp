#include <Columns/ColumnString.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeString.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>
#include <Common/UTF8Helpers.h>

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


private:
    void convertAndAppendCodePoint(int code_point, String & row_str) const;
    ContextPtr context;
};

void FunctionKqlMakeString::convertAndAppendCodePoint(const int code_point, String & row_str) const
{
    if (code_point < 0 || code_point > 1114111)
        throw DB::Exception(
            DB::ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
            "Argument in function {} is out of range, should be between 0 and 1114111",
            getName());

    std::array<char, 4> buff;
    const auto num_chars = UTF8::convertCodePointToUTF8(code_point, buff.data(), buff.size());
    row_str.append(buff.data(), num_chars);
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
                const auto len = arr_field.get<Array>().size();
                for (size_t k = 0; k < len; ++k)
                {
                    const auto & val = arr_field.get<Array>().at(k);
                    const auto code_point = static_cast<int>(val.get<Int64>());
                    convertAndAppendCodePoint(code_point, row_str);
                }
            }
            else
            {
                const auto code_point = static_cast<int>(arguments[i].column->getInt(j));
                convertAndAppendCodePoint(code_point, row_str);
            }
        }
        out_col->insertData(row_str.c_str(), row_str.size());
    }
    return out_col;
}

REGISTER_FUNCTION(KqlMakeString)
{
    factory.registerFunction<FunctionKqlMakeString>();
}
}
