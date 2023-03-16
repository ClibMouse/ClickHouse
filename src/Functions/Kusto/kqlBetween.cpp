#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeString.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>
#include <Parsers/Kusto/ParserKQLTimespan.h>
#include <DataTypes/DataTypeFactory.h>
#include <Columns/ColumnsNumber.h>


#include <format>

namespace DB
{

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

class FunctionKqlBetween : public IFunction
{
public:
    static constexpr auto name = "kql_between";
    static FunctionPtr create(ContextPtr context) { return std::make_shared<FunctionKqlBetween>(std::move(context)); }

    explicit FunctionKqlBetween(ContextPtr context_) : context(std::move(context_)) { }
    ~FunctionKqlBetween() override = default;

    ColumnPtr
    executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override;
    String getName() const override { return name; }
    size_t getNumberOfArguments() const override { return 3; }
    DataTypePtr getReturnTypeImpl(const DataTypes &) const override { return DataTypeFactory::instance().get("Bool"); }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }

private:
    ContextPtr context;
};
/*
TO DO Steps
1. make it work for normal values (int, decimal, DateTime etc...)
2. for DateTime and TimeSpan or other, make special case and make it work
3. Fix code in Operator cpp file --- add check for dots, params, brackets etc. and throw exception
4. make a new brunch in Kusto/ folder from latest phase3
5. Copy code from here to there
4. Run functional and unit test
5. Fix/Add test cases
6. Run all tests again
7. Submit a new PR
*/
ColumnPtr
FunctionKqlBetween::executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, const size_t input_rows_count) const
{
    if (arguments.size() < 3)
        throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Number of arguments in function {} doesn't match.", getName());
    const auto & base_arg = arguments[0]; // X
    const auto & comparable_arg1 = arguments[1]; // Y
    const auto & comparable_arg2 = arguments[2]; // Z  x >= y and x <= y + z
    
    auto new_column = ColumnUInt8::create(input_rows_count);
    
    bool add_interval = false;
    
    if (WhichDataType which_data_type(*comparable_arg2.type);which_data_type.isInterval())
        add_interval = true;
    std::cout << base_arg.column->getFamilyName() << std::endl;
    std::cout << base_arg.column->getName() << std::endl;
    //std::cout << base_arg.type->getTypeId() << std::endl;
    std::cout << "---------------------------" << std::endl;
    
    std::cout << comparable_arg1.column->getFamilyName() << std::endl;
    std::cout << comparable_arg1.column->getName() << std::endl;
    std::cout << "---------------------------" << std::endl;
    std::cout << comparable_arg2.column->getFamilyName() << std::endl;
    std::cout << comparable_arg2.column->getName() << std::endl;
    std::cout << "..........................." << std::endl;
    
    for (size_t i = 0; i < input_rows_count; ++i)
    {
        std::cout << "base_arg << (" << i << ") : " << base_arg.column->getInt(i) << std::endl; 
        std::cout << "comparable_arg1 << (" << i << ") : " << comparable_arg1.column->getInt(i) << std::endl; 
        std::cout << "comparable_arg2 << (" << i << ") : " << comparable_arg2.column->getInt(i) << std::endl; 
        if (add_interval)
            new_column->getData()[i] = (base_arg.column->getInt(i) >= comparable_arg1.column->getInt(i) && base_arg.column->getInt(i) <= comparable_arg2.column->getInt(i) + comparable_arg1.column->getInt(i));
        else
        {
            if (WhichDataType which_data_type2(*base_arg.type); which_data_type2.isInt() || which_data_type2.isDateOrDate32OrDateTimeOrDateTime64())
                new_column->getData()[i] = (base_arg.column->getInt(i) >= comparable_arg1.column->getInt(i) && base_arg.column->getInt(i) <= comparable_arg2.column->getInt(i));
            else if (WhichDataType which_data_type3(*base_arg.type); which_data_type3.isUInt())
                new_column->getData()[i] = (base_arg.column->getUInt(i) >= comparable_arg1.column->getUInt(i) && base_arg.column->getUInt(i) <= comparable_arg2.column->getUInt(i));
            else if (WhichDataType which_data_type4(*base_arg.type); which_data_type4.isDecimal() || which_data_type4.isFloat())
                new_column->getData()[i] = (base_arg.column->getFloat64(i) >= comparable_arg1.column->getFloat64(i) && base_arg.column->getFloat64(i) <= comparable_arg2.column->getFloat64(i));
        }
            
    }
   
    std::cout << "Inside FunctionKqlBetween" << std::endl;
    //return executeFunctionCall(context, "toUInt8", arguments, input_rows_count).first;
    
    //return executeFunctionCall(context, "and", {base_arg > comparable_arg1, base_arg < comparable_arg1}, input_rows_count).first;
    return new_column;
}

REGISTER_FUNCTION(KqlBetween)
{
    factory.registerFunction<FunctionKqlBetween>();
}
}
