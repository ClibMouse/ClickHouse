#include <string>
#include <Functions/Kusto/KqlFunctionBase.h>

namespace DB
{
namespace DB::ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

class FunctionKqlCountOverlappingSubstrings : public IFunction
{
public:
    static constexpr auto name = "kql_count_overlapping_substrings";
    static FunctionPtr create(ContextPtr context) { return std::make_shared<FunctionKqlCountOverlappingSubstrings>(std::move(context)); }

    explicit FunctionKqlCountOverlappingSubstrings(ContextPtr context_) : context(std::move(context_)) { }
    ~FunctionKqlCountOverlappingSubstrings() override = default;

    String getName() const override { return name; }
    size_t getNumberOfArguments() const override { return 2; }
    bool isVariadic() const override { return false; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        const auto args_length = arguments.size();

        if (args_length != 2)
        {
            throw Exception(
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "Number of arguments for function {} doesn't match: passed {}, should be 2.",
                getName(),
                toString(arguments.size()));
        }

        if (!isString(arguments.at(0).type))
        {
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Illegal type of argument of function {}", getName());
        }

        if (!isString(arguments.at(1).type))
        {
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Illegal type of argument of function {}", getName());
        }

        return std::make_shared<DataTypeUInt32>();
    }

    ColumnPtr executeImpl(
        const ColumnsWithTypeAndName & arguments,
        [[maybe_unused]] const DataTypePtr & result_type,
        const size_t input_rows_count) const override
    {
        auto result = ColumnUInt32::create();
        auto & result_column = result->getData();

        for (size_t i = 0; i < input_rows_count; ++i)
        {
            uint32_t res = 0;

            const auto source = arguments[0].column->getDataAt(i).toString();
            const auto search = arguments[1].column->getDataAt(i).toString();
            std::size_t found = 0;

            while ((found = source.find(search, found)) != std::string::npos)
            {
                ++res;
                ++found;
            }
            result_column.push_back(static_cast<UInt32>(res));
        }
        return result;
    }

private:
    ContextPtr context;
};

REGISTER_FUNCTION(KqlCountOverlappingSubstrings)
{
    factory.registerFunction<FunctionKqlCountOverlappingSubstrings>();
}
}
