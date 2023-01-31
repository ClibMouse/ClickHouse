#include <Columns/ColumnArray.h>
#include <Columns/ColumnTuple.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeTuple.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/Kusto/KqlFunctionBase.h>
#include <DataTypes/getLeastSupertype.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

class FunctionKqlArrayIif : public KqlFunctionBase
{
public:
    static constexpr auto name = "kql_ArrayIif";
    static FunctionPtr create(ContextPtr context) { return std::make_shared<FunctionKqlArrayIif>(std::move(context)); }
    explicit FunctionKqlArrayIif(ContextPtr context_) : context(std::move(context_)) { }
    ~FunctionKqlArrayIif() override = default;
    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 3; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }
    bool isVariadic() const override { return true; }
    bool useDefaultImplementationForConstants() const override { return true; }

    bool useDefaultImplementationForNulls() const override { return false; }
    bool useDefaultImplementationForNothing() const override { return false; }

    bool isDataTypeBoolORBoolConvertible(std::string_view datatype_name) const;

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override;
    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override;

private:
    ContextPtr context;
};

bool FunctionKqlArrayIif::isDataTypeBoolORBoolConvertible(std::string_view datatype_name) const
{
    if (datatype_name.find("Int") != datatype_name.npos ||
       datatype_name.find("Float") != datatype_name.npos ||
       datatype_name.find("Decimal") != datatype_name.npos ||
       datatype_name.find("Bool") != datatype_name.npos)
    return true;
    return false;
}

DataTypePtr FunctionKqlArrayIif::getReturnTypeImpl(const DataTypes & arguments) const
{
    const auto * array_type0 = typeid_cast<const DataTypeArray *>(arguments[0].get());
    if (!array_type0)
        throw Exception("First argument for function " + getName() + " must be an array but it has type "
                        + arguments[0]->getName() + ".", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

    DataTypePtr nested_type1, nested_type2;

    const auto * array_type1 = typeid_cast<const DataTypeArray *>(arguments[1].get());
    if (!array_type1)
        nested_type1 = makeNullable(arguments[1]->getPtr());
    else
        nested_type1 = makeNullable(array_type1->getNestedType());

    const auto * array_type2 = typeid_cast<const DataTypeArray *>(arguments[2].get());
    if (!array_type2)
        nested_type2 = makeNullable(arguments[2]->getPtr());
    else
        nested_type2 = makeNullable(array_type2->getNestedType());

    if (nested_type1->getName() != nested_type2->getName())
        throw Exception("Last two arguments for function " + getName() + " must have same nested data type "
                        + ".", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

    DataTypes types = {nested_type1, nested_type2};

    return std::make_shared<DataTypeArray>(getLeastSupertype(types));
}

ColumnPtr FunctionKqlArrayIif::executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, const size_t input_rows_count) const
{
    const DataTypePtr & elem_type = static_cast<const DataTypeArray &>(*result_type).getNestedType();
    auto out = ColumnArray::create(elem_type->createColumn());

    if (input_rows_count == 0)
        return out;

    IColumn & out_data = out->getData();
    IColumn::Offsets & out_offsets = out->getOffsets();
    size_t total_length = 0;
    for (size_t i = 0; i < input_rows_count; i++)
    {
        Field array0;
        arguments[0].column->get(i, array0);
        total_length += array0.get<Array>().size();
    }

    out_data.reserve(total_length);
    out_offsets.resize(input_rows_count);
    IColumn::Offset current_offset = 0;

    for (size_t i = 0; i < input_rows_count; i++)
    {
        Field array0;
        arguments[0].column->get(i, array0);
        size_t len0 = array0.get<Array>().size();
        for (size_t k = 0; k < len0; k++)
        {
            if (!isDataTypeBoolORBoolConvertible(array0.get<Array>().at(k).getTypeName()))
                out_data.insert(Field());
            else
            {
                Field temp;
                std::string dump = array0.get<Array>().at(k).dump();
                dump = dump.substr(dump.find("_") + 1);
                if (dump == "0" || dump == "-0")
                    arguments[2].column->get(i, temp);
                else
                    arguments[1].column->get(i, temp);
                if (temp.getTypeName() == "Array")
                {
                    if (k < temp.get<Array>().size())
                        out_data.insert(temp.get<Array>().at(k));
                    else
                        out_data.insert(Field());
                }
                else
                    out_data.insert(temp);
            }
        }
        current_offset += len0;
        out_offsets[i] = current_offset;
    }
    return out;
}

REGISTER_FUNCTION(KqlArrayIif)
{
    factory.registerFunction<FunctionKqlArrayIif>();
}

}
