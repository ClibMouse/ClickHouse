#include <Columns/ColumnArray.h>
#include <Columns/ColumnTuple.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeTuple.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/Kusto/KqlFunctionBase.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

class FunctionKqlIndexOf : public KqlFunctionBase
{
public:
    static constexpr auto name = "kql_indexof";
    explicit FunctionKqlIndexOf(ContextPtr context_) : context(context_) { }
    static FunctionPtr create(ContextPtr context) { return std::make_shared<FunctionKqlIndexOf>(context); }

    String getName() const override { return name; }

    bool isVariadic() const override { return true; }
    size_t getNumberOfArguments() const override { return 0; }
    bool useDefaultImplementationForConstants() const override { return true; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        if (arguments.size() < 2 || 5 < arguments.size())
            throw Exception(
                "Number of arguments for function " + getName() + " doesn't match: passed " + toString(arguments.size())
                    + ", should be 2 to 5.",
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        if (arguments.size() >= 3)
        {
            for (size_t i = 3; i < arguments.size(); ++i)
                if (!isInteger(arguments.at(i).type))
                    throw Exception("Illegal type of argument of function " + getName(), ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
        }

        return std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt64>());
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override
    {
        int64_t occurrence = 1;

        auto null_type = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeUInt64>());
        auto null_column = null_type->createColumnConstWithDefaultValue(1);
        auto not_found_column = DataTypeUInt64().createColumnConst(1, toField(UInt64(0)));

        ColumnPtr column_source = arguments[0].column;
        ColumnPtr column_lookup = arguments[1].column;
        ColumnPtr column_start_pos = DataTypeUInt64().createColumnConst(input_rows_count, toField(UInt64(1)));
        ColumnPtr column_length = DataTypeInt64().createColumnConst(input_rows_count, toField(Int64(-1)));

        if (!isString(arguments[0].type))
            column_source = FunctionFactory::instance()
                                .get("toString", context)
                                ->build({arguments[0]})
                                ->execute({arguments[0]}, std::make_shared<DataTypeString>(), input_rows_count);

        if (!isString(arguments[1].type))
            column_lookup = FunctionFactory::instance()
                                .get("toString", context)
                                ->build({arguments[1]})
                                ->execute({arguments[1]}, std::make_shared<DataTypeString>(), input_rows_count);

        if (arguments.size() >= 3)
        {
            auto input_start_column = ColumnUInt64::create();
            for (size_t j = 0; j < input_rows_count; ++j)
            {
                StringRef source = column_source->getDataAt(j);
                auto start_pos = arguments[2].column->getInt(j);
                if (start_pos < 0)
                {
                    start_pos = source.size + start_pos;
                    if (start_pos < 0)
                        start_pos = 0;
                }
                ++start_pos;
                input_start_column->insertValue(start_pos);
            }
            column_start_pos = std::move(input_start_column);
        }

        if (arguments.size() >= 4)
            column_length = arguments[3].column;

        if (arguments.size() == 5)
            occurrence = arguments[4].column->getInt(0); //must be a constant

        if (occurrence < 0)
            return null_column;

        ColumnPtr last_pos = not_found_column;
        for (auto i = 0; i < occurrence; ++i)
        {
            ColumnsWithTypeAndName position_args(
                {{ColumnWithTypeAndName(column_source, std::make_shared<DataTypeString>(), "source")},
                 {ColumnWithTypeAndName(column_lookup, std::make_shared<DataTypeString>(), "lookup")},
                 {ColumnWithTypeAndName(column_start_pos, std::make_shared<DataTypeUInt64>(), "start_pos")}});
            auto pos = FunctionFactory::instance()
                           .get("position", context)
                           ->build(position_args)
                           ->execute(position_args, result_type, input_rows_count);
            last_pos = pos;

            auto new_pos_column = ColumnUInt64::create();
            for (size_t j = 0; j < input_rows_count; ++j)
            {
                new_pos_column->insertValue(pos->getInt(j) + 1);
            }
            column_start_pos = std::move(new_pos_column);
        }

        auto null_map = ColumnUInt8::create(input_rows_count);
        auto result_column = ColumnInt64::create();
        for (size_t i = 0; i < input_rows_count; ++i)
        {
            auto length = column_length->getInt(i);
            null_map->getData()[i] = length < -1;

            auto pos_val = last_pos->get64(i);
            if (length > -1 && last_pos->get64(i) > UInt64(length) + 1)
                pos_val = 0;
            result_column->insertValue(Int64(pos_val) -1);  // used for kql, so returned index is 0 based
        }
        return ColumnNullable::create(std::move(result_column), std::move(null_map));
    }

private:
    ContextPtr context;
};

REGISTER_FUNCTION(KqlIndexOf)
{
    factory.registerFunction<FunctionKqlIndexOf>();
}

}
