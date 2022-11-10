#include <Columns/ColumnArray.h>
#include <Columns/ColumnTuple.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeTuple.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/Kusto/KqlFunctionBase.h>

namespace DB
{

template <typename Name, bool is_desc>
class FunctionKqlArraySort : public KqlFunctionBase
{
public:
    static constexpr auto name = Name::name;
    explicit FunctionKqlArraySort(ContextPtr context_) : context(context_) { }
    static FunctionPtr create(ContextPtr context) { return std::make_shared<FunctionKqlArraySort>(context); }

    String getName() const override { return name; }

    bool isVariadic() const override { return true; }
    size_t getNumberOfArguments() const override { return 0; }
    bool useDefaultImplementationForConstants() const override { return true; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        if (arguments.empty())
            throw Exception(
                "Function " + getName() + " needs at least one argument; passed " + toString(arguments.size()) + ".",
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        auto array_count = arguments.size();

        if (!isArray(arguments.at(array_count - 1).type))
            --array_count;

        DataTypes nested_types;
        for (size_t index = 0; index < array_count; ++index)
        {
            const DataTypeArray * array_type = checkAndGetDataType<DataTypeArray>(arguments[index].type.get());
            if (!array_type)
                throw Exception(
                    "Argument " + toString(index + 1) + " of function " + getName() + " must be array. Found "
                        + arguments[0].type->getName() + " instead.",
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
            nested_types.emplace_back(array_type->getNestedType());
        }

        DataTypes data_types(array_count);

        for (size_t i = 0; i < array_count; ++i)
            data_types[i] = std::make_shared<DataTypeArray>(makeNullable(nested_types[i]));

        return std::make_shared<DataTypeTuple>(data_types);
    }

 /*   ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override
    {
        if (input_rows_count == 0)
            return result_type->createColumn();

        if (input_rows_count > 1)
            std::cout<< 1; 
        size_t array_count = arguments.size();
        auto & last_arg = arguments[array_count - 1];
        DataTypes nested_types;
        String sort_function = is_desc ? "arrayReverseSort" : "arraySort";

        size_t input_rows_count_ = 1;

        if (!isArray(last_arg.type))
            --array_count;
        std::vector<Columns> tuple_columns(input_rows_count,Columns(array_count));

     //   ColumnArray::ColumnOffsets::MutablePtr result_offsets_column;
     //   result_offsets_column = ColumnArray::ColumnOffsets::create(input_rows_count);

        for (size_t row_num = 0; row_num < input_rows_count; ++row_num)
        {
            bool null_last = true;
            if (!isArray(last_arg.type))
            {
                null_last = last_arg.column->getBool(row_num);
                 // std::cout<<r;
                //null_last = check_condition(last_arg, context, input_rows_count_);
            }

            ColumnsWithTypeAndName new_args;
            ColumnPtr first_array_column;
            std::unordered_set<size_t> null_indices;
            

            for (size_t i = 0; i < array_count; ++i)
            {
                ColumnPtr holder = arguments[i].column->convertToFullColumnIfConst();

                const ColumnArray * column_array = checkAndGetColumn<ColumnArray>(holder.get());
                const DataTypeArray * array_type = checkAndGetDataType<DataTypeArray>(arguments[i].type.get());

                if (!column_array)
                    throw Exception(
                        "Argument " + toString(i + 1) + " of function " + getName()
                            + " must be array."
                            " Found column "
                            + holder->getName() + " instead.",
                        ErrorCodes::ILLEGAL_COLUMN);

                nested_types.emplace_back(makeNullable(array_type->getNestedType()));


                auto get_single_row = [&]
                {
                    const ColumnArray * src_col = checkAndGetColumn<ColumnArray>(arguments[i].column.get());
                    const IColumn::Offsets & src_offsets = src_col->getOffsets();
                    const IColumn::Offsets * prev_offsets = &src_offsets;
                    const IColumn * prev_data = &src_col->getData();

                    ColumnArray::ColumnOffsets::MutablePtr single_row_offsets_column;

                    single_row_offsets_column = ColumnArray::ColumnOffsets::create(1);

                    IColumn::Offsets & single_offsets = single_row_offsets_column->getData();
                    single_offsets[0] = (*prev_offsets)[row_num];
                    auto single_column = ColumnArray::create(prev_data->getPtr(),std::move(single_row_offsets_column));
                    ColumnWithTypeAndName single_row_args{single_column, std::make_shared<DataTypeArray>(nested_types[i]), "array"};
                    return single_row_args;
                };
                if (i == 0)
                {
                    first_array_column = holder;
                    new_args.push_back(get_single_row());
                    //new_args.push_back(arguments[i]);
                }
                else if (!column_array->hasEqualOffsets(static_cast<const ColumnArray &>(*first_array_column)))
                {
                    null_indices.insert(i);
                }
                else
                    new_args.push_back(get_single_row());
                    //new_args.push_back(arguments[i]);

            }

            auto zipped
                = FunctionFactory::instance().get("arrayZip", context)->build(new_args)->execute(new_args, result_type, input_rows_count_);

            ColumnsWithTypeAndName sort_arg({{zipped, std::make_shared<DataTypeArray>(result_type), "zipped"}});
            auto sorted_tuple
                = FunctionFactory::instance().get(sort_function, context)->build(sort_arg)->execute(sort_arg, result_type, input_rows_count_);

            auto null_type = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt8>());

            
            size_t sorted_index = 0;
            for (size_t i = 0; i < array_count; ++i)
            {
                if (null_indices.contains(i))
                {
                    auto fun_array = FunctionFactory::instance().get("array", context);

                    DataTypePtr arg_type
                        = std::make_shared<DataTypeArray>(makeNullable(nested_types[i]));

                    ColumnsWithTypeAndName null_array_arg({
                        {null_type->createColumnConstWithDefaultValue(input_rows_count_), null_type, "NULL"},
                    });

                    tuple_columns[row_num][i] = fun_array->build(null_array_arg)->execute(null_array_arg, arg_type, input_rows_count_);
                    tuple_columns[row_num][i] = tuple_columns[row_num][i]->convertToFullColumnIfConst();
                }
                else
                {
                    ColumnsWithTypeAndName untuple_args(
                        {{ColumnWithTypeAndName(sorted_tuple, std::make_shared<DataTypeArray>(result_type), "sorted")},
                        {DataTypeUInt8().createColumnConst(1, toField(UInt8(sorted_index + 1))), std::make_shared<DataTypeUInt8>(), ""}});
                    auto tuple_coulmn = FunctionFactory::instance()
                                            .get("tupleElement", context)
                                            ->build(untuple_args)
                                            ->execute(untuple_args, result_type, input_rows_count_);

                    auto out_tmp = ColumnArray::create(nested_types[i]->createColumn());

                    size_t array_size = tuple_coulmn->size();
                    auto * arr = checkAndGetColumn<ColumnArray>(tuple_coulmn.get());

                    for (size_t j = 0; j < array_size; ++j)
                    {
                        Field arr_field;
                        arr->get(j, arr_field);
                        out_tmp->insert(arr_field);
                    }

                    tuple_columns[row_num][i] = std::move(out_tmp);

                    ++sorted_index;
                }
            }

            if (!null_last)
            {
                //Columns adjusted_columns(array_count);
                std::vector<Columns> adjusted_columns(input_rows_count,Columns(array_count));

                ColumnWithTypeAndName arg_of_index{nullptr, std::make_shared<DataTypeArray>(nested_types[0]), "array"};
                arg_of_index.column = tuple_columns[row_num][0];

                auto inside_null_type = nested_types[0];
                ColumnsWithTypeAndName indexof_args({
                    arg_of_index,
                    {inside_null_type->createColumnConstWithDefaultValue(input_rows_count_), inside_null_type, "NULL"},
                });

                auto null_index_datetype = std::make_shared<DataTypeUInt64>();

                ColumnWithTypeAndName slice_index{nullptr, null_index_datetype, ""};
                slice_index.column = FunctionFactory::instance()
                                        .get("indexOf", context)
                                        ->build(indexof_args)
                                        ->execute(indexof_args, result_type, input_rows_count_);

                auto null_index_in_array = slice_index.column->get64(0);
                if (null_index_in_array > 0)
                {
                    ColumnWithTypeAndName slice_index_len{nullptr, null_index_datetype, ""};
                    slice_index_len.column = DataTypeUInt64().createColumnConst(1, toField(UInt64(null_index_in_array - 1)));

                    auto fun_slice = FunctionFactory::instance().get("arraySlice", context);

                    for (size_t i = 0; i < array_count; ++i)
                    {
                        if (null_indices.contains(i))
                        {
                            adjusted_columns[row_num][i] = std::move(tuple_columns[row_num][i]);
                        }
                        else
                        {
                            DataTypePtr arg_type = std::make_shared<DataTypeArray>(nested_types[i]);

                            ColumnsWithTypeAndName slice_args_left(
                                {{ColumnWithTypeAndName(tuple_columns[row_num][i], arg_type, "array")},
                                {DataTypeUInt8().createColumnConst(1, toField(UInt8(1))), std::make_shared<DataTypeUInt8>(), ""},
                                slice_index_len});

                            ColumnsWithTypeAndName slice_args_right(
                                {{ColumnWithTypeAndName(tuple_columns[row_num][i], arg_type, "array")}, slice_index});
                            ColumnWithTypeAndName arr_left{
                                fun_slice->build(slice_args_left)->execute(slice_args_left, arg_type, input_rows_count_), arg_type, ""};
                            ColumnWithTypeAndName arr_right{
                                fun_slice->build(slice_args_right)->execute(slice_args_right, arg_type, input_rows_count_), arg_type, ""};

                            ColumnsWithTypeAndName arr_cancat({arr_right, arr_left});
                            auto out_tmp = FunctionFactory::instance()
                                            .get("arrayConcat", context)
                                            ->build(arr_cancat)
                                            ->execute(arr_cancat, arg_type, input_rows_count_);
                            adjusted_columns[row_num][i] = std::move(out_tmp);
                        }
                       
                    }
                   // return ColumnTuple::create(adjusted_columns);
                    tuple_columns = std::move(adjusted_columns);
                }
            }
            //return ColumnTuple::create(tuple_columns);
            // do insert here
        }

        Columns total_tuple_columns(array_count);
        for (size_t i = 0; i < array_count; ++i)
        {
            auto out_tmp = ColumnArray::create(nested_types[i]->createColumn());

            IColumn::Offsets & out_offsets = out_tmp->getOffsets();
            IColumn::Offset current_offset = 0;
            for (size_t row = 0; row < input_rows_count; ++row)
            {
                size_t array_size = tuple_columns[row][i]->size();
                auto * arr = checkAndGetColumn<ColumnArray>(tuple_columns[row][i].get());
                const IColumn::Offsets & src_offsets = arr->getOffsets();

                for (size_t j = 0; j < array_size; ++j)
                {
                    Field arr_field;
                    arr->get(j, arr_field);
                    out_tmp->insert(arr_field);
                }
                current_offset += src_offsets[0];
                out_offsets[row] = current_offset;
            }

            total_tuple_columns[i] = std::move(out_tmp);
        }

        return ColumnTuple::create(total_tuple_columns);
    }
*/

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override
    {
        size_t array_count = arguments.size();
        auto & last_arg = arguments[array_count - 1];

        size_t input_rows_count_ = input_rows_count;

        bool null_last = true;
        if (!isArray(last_arg.type))
        {
            --array_count;
            null_last = check_condition(last_arg, context, input_rows_count_);
        }

        ColumnsWithTypeAndName new_args;
        ColumnPtr first_array_column;
        std::unordered_set<size_t> null_indices;
        DataTypes nested_types;

        String sort_function = is_desc ? "arrayReverseSort" : "arraySort";

        for (size_t i = 0; i < array_count; ++i)
        {
            ColumnPtr holder = arguments[i].column->convertToFullColumnIfConst();

            const ColumnArray * column_array = checkAndGetColumn<ColumnArray>(holder.get());
            const DataTypeArray * array_type = checkAndGetDataType<DataTypeArray>(arguments[i].type.get());

            if (!column_array)
                throw Exception(
                    "Argument " + toString(i + 1) + " of function " + getName()
                        + " must be array."
                          " Found column "
                        + holder->getName() + " instead.",
                    ErrorCodes::ILLEGAL_COLUMN);

            nested_types.emplace_back(makeNullable(array_type->getNestedType()));
            if (i == 0)
            {
                first_array_column = holder;
                new_args.push_back(arguments[i]);
            }
            else if (!column_array->hasEqualOffsets(static_cast<const ColumnArray &>(*first_array_column)))
            {
                null_indices.insert(i);
            }
            else
                new_args.push_back(arguments[i]);
        }

        auto zipped
            = FunctionFactory::instance().get("arrayZip", context)->build(new_args)->execute(new_args, result_type, input_rows_count_);

        ColumnsWithTypeAndName sort_arg({{zipped, std::make_shared<DataTypeArray>(result_type), "zipped"}});
        auto sorted_tuple
            = FunctionFactory::instance().get(sort_function, context)->build(sort_arg)->execute(sort_arg, result_type, input_rows_count_);

        auto null_type = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt8>());

        Columns tuple_columns(array_count);
        size_t sorted_index = 0;
        for (size_t i = 0; i < array_count; ++i)
        {
            if (null_indices.contains(i))
            {
                auto fun_array = FunctionFactory::instance().get("array", context);

                DataTypePtr arg_type
                    = std::make_shared<DataTypeArray>(makeNullable(nested_types[i]));

                ColumnsWithTypeAndName null_array_arg({
                    {null_type->createColumnConstWithDefaultValue(input_rows_count_), null_type, "NULL"},
                });

                tuple_columns[i] = fun_array->build(null_array_arg)->execute(null_array_arg, arg_type, input_rows_count_);
                tuple_columns[i] = tuple_columns[i]->convertToFullColumnIfConst();
            }
            else
            {
                ColumnsWithTypeAndName untuple_args(
                    {{ColumnWithTypeAndName(sorted_tuple, std::make_shared<DataTypeArray>(result_type), "sorted")},
                     {DataTypeUInt8().createColumnConst(1, toField(UInt8(sorted_index + 1))), std::make_shared<DataTypeUInt8>(), ""}});
                auto tuple_coulmn = FunctionFactory::instance()
                                        .get("tupleElement", context)
                                        ->build(untuple_args)
                                        ->execute(untuple_args, result_type, input_rows_count_);

                auto out_tmp = ColumnArray::create(nested_types[i]->createColumn());

                size_t array_size = tuple_coulmn->size();
                auto * arr = checkAndGetColumn<ColumnArray>(tuple_coulmn.get());

                for (size_t j = 0; j < array_size; ++j)
                {
                    Field arr_field;
                    arr->get(j, arr_field);
                    out_tmp->insert(arr_field);
                }

                tuple_columns[i] = std::move(out_tmp);

                ++sorted_index;
            }
        }

        if (!null_last)
        {
            Columns adjusted_columns(array_count);

            ColumnWithTypeAndName arg_of_index{nullptr, std::make_shared<DataTypeArray>(nested_types[0]), "array"};
            arg_of_index.column = tuple_columns[0];

            auto inside_null_type = nested_types[0];
            ColumnsWithTypeAndName indexof_args({
                arg_of_index,
                {inside_null_type->createColumnConstWithDefaultValue(input_rows_count_), inside_null_type, "NULL"},
            });

            auto null_index_datetype = std::make_shared<DataTypeUInt64>();

            ColumnWithTypeAndName slice_index{nullptr, null_index_datetype, ""};
            slice_index.column = FunctionFactory::instance()
                                     .get("indexOf", context)
                                     ->build(indexof_args)
                                     ->execute(indexof_args, result_type, input_rows_count_);

            auto null_index_in_array = slice_index.column->get64(0);
            if (null_index_in_array > 0)
            {
                ColumnWithTypeAndName slice_index_len{nullptr, null_index_datetype, ""};
                slice_index_len.column = DataTypeUInt64().createColumnConst(1, toField(UInt64(null_index_in_array - 1)));

                auto fun_slice = FunctionFactory::instance().get("arraySlice", context);

                for (size_t i = 0; i < array_count; ++i)
                {
                    if (null_indices.contains(i))
                    {
                        adjusted_columns[i] = std::move(tuple_columns[i]);
                    }
                    else
                    {
                        DataTypePtr arg_type = std::make_shared<DataTypeArray>(nested_types[i]);

                        ColumnsWithTypeAndName slice_args_left(
                            {{ColumnWithTypeAndName(tuple_columns[i], arg_type, "array")},
                             {DataTypeUInt8().createColumnConst(1, toField(UInt8(1))), std::make_shared<DataTypeUInt8>(), ""},
                             slice_index_len});

                        ColumnsWithTypeAndName slice_args_right(
                            {{ColumnWithTypeAndName(tuple_columns[i], arg_type, "array")}, slice_index});
                        ColumnWithTypeAndName arr_left{
                            fun_slice->build(slice_args_left)->execute(slice_args_left, arg_type, input_rows_count_), arg_type, ""};
                        ColumnWithTypeAndName arr_right{
                            fun_slice->build(slice_args_right)->execute(slice_args_right, arg_type, input_rows_count_), arg_type, ""};

                        ColumnsWithTypeAndName arr_cancat({arr_right, arr_left});
                        auto out_tmp = FunctionFactory::instance()
                                           .get("arrayConcat", context)
                                           ->build(arr_cancat)
                                           ->execute(arr_cancat, arg_type, input_rows_count_);
                        adjusted_columns[i] = std::move(out_tmp);
                    }
                }
                return ColumnTuple::create(adjusted_columns);
            }
        }
        return ColumnTuple::create(tuple_columns);
    }

private:
    ContextPtr context;
};

struct NameKqlArraySortAsc
{
    static constexpr auto name = "kql_array_sort_asc";
};

struct NameKqlArraySortDesc
{
    static constexpr auto name = "kql_array_sort_desc";
};

using FunctionKqlArraySortAsc = FunctionKqlArraySort<NameKqlArraySortAsc, false>;
using FunctionKqlArraySortDesc = FunctionKqlArraySort<NameKqlArraySortDesc, true>;

REGISTER_FUNCTION(KqlArraySort)
{
    factory.registerFunction<FunctionKqlArraySortAsc>();
    factory.registerFunction<FunctionKqlArraySortDesc>();
}

}
