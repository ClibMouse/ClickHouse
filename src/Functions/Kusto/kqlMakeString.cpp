#include <Columns/ColumnString.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeString.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>

#include <format>

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
    static void addArgs(ColumnWithTypeAndName & column_source, ColumnsWithTypeAndName & new_args, auto & out_col);

private:
    ContextPtr context;
};

void FunctionKqlMakeString::addArgs(ColumnWithTypeAndName & column_source, ColumnsWithTypeAndName & new_args, auto & out_col)
{
    column_source.column = std::move(out_col);
    column_source.type = std::make_shared<DataTypeString>();
    column_source.name = "column_source";
    new_args.push_back(column_source);
    return;
}

DataTypePtr FunctionKqlMakeString::getReturnTypeImpl(const DataTypes & arguments) const
{
    if (const auto argument_count = std::ssize(arguments); argument_count < 1 || argument_count > 64)
        throw DB::Exception(
            DB::ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
            "Number of arguments for function {} doesn't match: passed {}, should be between 1 and 64",
            getName(),
            argument_count);

    bool args_legal = true;

    for (size_t i = 0; i < arguments.size(); ++i)
    {
        if (const auto * array_type = typeid_cast<const DataTypeArray *>(arguments[i].get()); array_type)
        {
            WhichDataType which(array_type->getNestedType()->getPtr());

            if (!which.isUInt() && !which.isInt() && !which.isNothing())
            {
                args_legal = false;
                break;
            }
        }
        else if (!WhichDataType(arguments[i]).isUInt() && !WhichDataType(arguments[i]).isInt())
        {
            args_legal = false;
            break;
        }
    }

    if (!args_legal)
        throw DB::Exception(
            DB::ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
            "Arguments type for function {} doesn't match: arguments should be integers int,long or a dynamic value holding an array of "
            "integral numbers",
            getName());

    return std::make_shared<DataTypeString>();
}

ColumnPtr
FunctionKqlMakeString::executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, const size_t input_rows_count) const
{
    ColumnWithTypeAndName column_source;
    ColumnsWithTypeAndName new_args;
    auto func = FunctionFactory::instance().get("char", context);

    auto blank_col = ColumnString::create();

    for (size_t j = 0; j < input_rows_count; ++j)
        blank_col->insert(toField(String("")));

    addArgs(column_source, new_args, blank_col);
    for (size_t i = 0; i < arguments.size(); ++i)
    {
        if (WhichDataType(arguments[i].type).isArray())
        {
            auto out_col = ColumnString::create();

            for (size_t j = 0; j < input_rows_count; ++j)
            {
                Field arr_field;
                arguments[i].column->get(j, arr_field);
                size_t len = arr_field.get<Array>().size();
                String temp_str;
                for (size_t k = 0; k < len; ++k)
                {
                    Field val = arr_field.get<Array>().at(k);
                    temp_str += static_cast<char>(val.get<UInt64>());
                }
                out_col->insert(toField(temp_str));
            }
            addArgs(column_source, new_args, out_col);
        }
        else
        {
            column_source.column = func->build({arguments[i]})
                                       ->execute({arguments[i]}, std::make_shared<DataTypeString>(), input_rows_count)
                                       ->convertToFullColumnIfConst();
            column_source.type = std::make_shared<DataTypeString>();
            column_source.name = "column_source";
            new_args.push_back(column_source);
        }
    }
    return executeFunctionCall(context, "concat", new_args, input_rows_count).first;
}

REGISTER_FUNCTION(KqlMakeString)
{
    factory.registerFunction<FunctionKqlMakeString>();
}
}
