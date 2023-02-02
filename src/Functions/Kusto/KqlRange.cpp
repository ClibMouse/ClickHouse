#include <numeric>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnDecimal.h>
#include <Columns/ColumnVector.h>
#include <Columns/ColumnsDateTime.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeInterval.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/getLeastSupertype.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>
#include <Functions/Kusto/KqlFunctionBase.h>
#include <Interpreters/Context.h>
#include <Interpreters/castColumn.h>
#include <Common/IntervalKind.h>
namespace DB
{
namespace ErrorCodes
{
    extern const int ARGUMENT_OUT_OF_BOUND;
    extern const int ILLEGAL_COLUMN;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

static constexpr size_t max_array_size_as_field = 1000000; // the value from ColumnArray.cpp

class FunctionKqlRange : public IFunction
{
public:
    static constexpr auto name = "kql_range";

    const size_t max_elements;
    static FunctionPtr create(ContextPtr context_) { return std::make_shared<FunctionKqlRange>(std::move(context_)); }
    explicit FunctionKqlRange(ContextPtr context) : max_elements(context->getSettingsRef().function_range_max_elements_in_block) { }

private:
    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 0; }
    bool isVariadic() const override { return true; }
    bool useDefaultImplementationForConstants() const override { return true; }
    bool useDefaultImplementationForNulls() const override { return false; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (arguments.size() < 2 || 3 < arguments.size())
            throw Exception(
                "Number of arguments for function " + getName() + " doesn't match: passed " + toString(arguments.size())
                    + ", should be 2 or 3.",
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        const auto & start = arguments[0];
        const auto & end = arguments[1];

        WhichDataType start_type(*start);
        WhichDataType end_type(*end);

        auto return_type = start;
        if (start_type.isNullable())
        {
            const auto * nullable_type = checkAndGetDataType<DataTypeNullable>(start.get());
            if (nullable_type)
            {
                return_type = nullable_type->getNestedType();
                start_type = WhichDataType(nullable_type->getNestedType());
            }
        }
        if (end_type.isNullable())
        {
            const auto * nullable_type = checkAndGetDataType<DataTypeNullable>(end.get());
            if (nullable_type)
            {
                end_type = WhichDataType(nullable_type->getNestedType());
            }
        }
        if ((!start_type.isDateTime64()) && !start_type.isInterval() && !isNumber(start_type))
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Illegal type of first argument of function {}, expected DateTime64, Interval or Number",
                getName());

        if ((start_type.isDateTime64() && !end_type.isDateTime64()) || (!start_type.isDateTime64() && end_type.isDateTime64()))
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Type not match of first and second argument of function {}", getName());

        if ((start_type.isInterval() && !end_type.isInterval()) || (!start_type.isInterval() && end_type.isInterval()))
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Type not match of first and second argument of function {}", getName());

        if (arguments.size() == 3)
        {
            const auto & step = arguments[2];
            const WhichDataType step_type(*step);
            if (!isNumber(step_type) && !step_type.isInterval())
                throw Exception(
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                    "Illegal type of third argument of function {}, expected Interval or Number",
                    getName());
        }

        if (start_type.isDateTime64() || start_type.isInterval())
            return std::make_shared<DataTypeArray>(return_type);
        DataTypePtr common_type = getLeastSupertype(arguments);
        return std::make_shared<DataTypeArray>(common_type);
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override
    {
        DataTypePtr elem_type = checkAndGetDataType<DataTypeArray>(result_type.get())->getNestedType();
        WhichDataType which(elem_type);
        ColumnPtr res;
        const auto & start = arguments[0];
        WhichDataType start_type(*start.type);

        ColumnsWithTypeAndName new_args;

        for (size_t i = 0; i < arguments.size(); ++i)
        {
            const auto & arg_col = arguments[i];
            WhichDataType arg_type(*arg_col.type);

            if (arg_type.isNullable())
            {
                const auto * nullable_type = checkAndGetDataType<DataTypeNullable>(arg_col.type.get());
                const auto & nested_type = nullable_type->getNestedType();
                const auto * nullable_column = checkAndGetColumn<ColumnNullable>(*arguments[i].column);
                ColumnPtr nested_column = nullable_column->getNestedColumnPtr();
                ColumnWithTypeAndName new_arg{nullptr, nested_type, "new_arg"};
                new_arg.column = nested_column;
                new_args.push_back(new_arg);

                if (i == 0)
                    start_type = nested_type;
            }
            else
                new_args.push_back(arguments[i]);
        }

        if (start_type.isDateTime64())
        {
            return executeDateTime64(new_args, result_type, input_rows_count);
        }

        if (start_type.isInterval())
        {
            return executeInterval(new_args, result_type, input_rows_count);
        }

        Columns columns_holder(3);
        ColumnRawPtrs column_ptrs(3);

        for (size_t i = 0; i < new_args.size(); ++i)
        {
            if (i <= 1)
                columns_holder[i] = castColumn(new_args[i], elem_type)->convertToFullColumnIfConst();
            else
                columns_holder[i] = castColumn(new_args[i], elem_type);

            column_ptrs[i] = columns_holder[i].get();
        }

        /// Step is one by default.
        if (new_args.size() == 2)
        {
            /// Convert a column with constant 1 to the result type.
            if (start_type.isFloat32())
                columns_holder[2] = castColumn(
                    {DataTypeFloat32().createColumnConst(input_rows_count, 1.0), std::make_shared<DataTypeFloat32>(), {}}, elem_type);
            else if (start_type.isFloat64())
                columns_holder[2] = castColumn(
                    {DataTypeFloat64().createColumnConst(input_rows_count, 1.0), std::make_shared<DataTypeFloat64>(), {}}, elem_type);
            else if (start_type.isUInt8() || start_type.isUInt16() || start_type.isUInt32() || start_type.isUInt64())
                columns_holder[2] = castColumn(
                    {DataTypeUInt8().createColumnConst(input_rows_count, 1), std::make_shared<DataTypeUInt8>(), {}}, elem_type);
            else
                columns_holder[2]
                    = castColumn({DataTypeInt8().createColumnConst(input_rows_count, 1), std::make_shared<DataTypeInt8>(), {}}, elem_type);

            column_ptrs[2] = columns_holder[2].get();
        }

        bool is_start_const = isColumnConst(*column_ptrs[0]);
        bool is_step_const = isColumnConst(*column_ptrs[2]);

        if (is_start_const && is_step_const)
        {
            UInt64 start_uint = assert_cast<const ColumnConst &>(*column_ptrs[0]).getUInt(0);
            UInt64 step_uint = assert_cast<const ColumnConst &>(*column_ptrs[2]).getUInt(0);
            Int64 start_int = assert_cast<const ColumnConst &>(*column_ptrs[0]).getInt(0);
            Int64 step_int = assert_cast<const ColumnConst &>(*column_ptrs[2]).getInt(0);
            Float32 start_float32 = assert_cast<const ColumnConst &>(*column_ptrs[0]).getFloat32(0);
            Float32 step_float32 = assert_cast<const ColumnConst &>(*column_ptrs[2]).getFloat32(0);
            Float64 start_float64 = assert_cast<const ColumnConst &>(*column_ptrs[0]).getFloat64(0);
            Float64 step_float64 = assert_cast<const ColumnConst &>(*column_ptrs[2]).getFloat64(0);

            if ((res = executeConstStartStep<UInt8>(column_ptrs[1], start_uint, step_uint, input_rows_count))
                || (res = executeConstStartStep<UInt16>(column_ptrs[1], start_uint, step_uint, input_rows_count))
                || (res = executeConstStartStep<UInt32>(
                        column_ptrs[1], static_cast<UInt32>(start_uint), static_cast<UInt32>(step_uint), input_rows_count))
                || (res = executeConstStartStep<UInt64>(column_ptrs[1], start_uint, step_uint, input_rows_count))
                || (res = executeConstStartStep<Int8>(column_ptrs[1], start_int, step_int, input_rows_count))
                || (res = executeConstStartStep<Int16>(column_ptrs[1], start_int, step_int, input_rows_count))
                || (res = executeConstStartStep<Int32>(
                        column_ptrs[1], static_cast<Int32>(start_uint), static_cast<Int32>(step_uint), input_rows_count))
                || (res = executeConstStartStep<Int64>(column_ptrs[1], start_int, step_int, input_rows_count))
                || (res = executeConstStartStep<Float32>(column_ptrs[1], start_float32, step_float32, input_rows_count))
                || (res = executeConstStartStep<Float64>(column_ptrs[1], start_float64, step_float64, input_rows_count)))
            {
            }
        }
        else if (is_start_const && !is_step_const)
        {
            UInt64 start_uint = assert_cast<const ColumnConst &>(*column_ptrs[0]).getUInt(0);
            Int64 start_int = assert_cast<const ColumnConst &>(*column_ptrs[0]).getInt(0);
            Float32 start_float32 = assert_cast<const ColumnConst &>(*column_ptrs[0]).getFloat32(0);
            Float64 start_float64 = assert_cast<const ColumnConst &>(*column_ptrs[0]).getFloat64(0);

            if ((res = executeConstStart<UInt8>(column_ptrs[1], column_ptrs[2], start_uint, input_rows_count))
                || (res = executeConstStart<UInt16>(column_ptrs[1], column_ptrs[2], start_uint, input_rows_count))
                || (res = executeConstStart<UInt32>(column_ptrs[1], column_ptrs[2], static_cast<Int32>(start_uint), input_rows_count))
                || (res = executeConstStart<UInt64>(column_ptrs[1], column_ptrs[2], start_uint, input_rows_count))
                || (res = executeConstStart<Int8>(column_ptrs[1], column_ptrs[2], start_int, input_rows_count))
                || (res = executeConstStart<Int16>(column_ptrs[1], column_ptrs[2], start_int, input_rows_count))
                || (res = executeConstStart<Int32>(column_ptrs[1], column_ptrs[2], static_cast<Int32>(start_uint), input_rows_count))
                || (res = executeConstStart<Int64>(column_ptrs[1], column_ptrs[2], start_int, input_rows_count))
                || (res = executeConstStart<Float32>(column_ptrs[1], column_ptrs[2], start_float32, input_rows_count))
                || (res = executeConstStart<Float64>(column_ptrs[1], column_ptrs[2], start_float64, input_rows_count)))
            {
            }
        }
        else if (!is_start_const && is_step_const)
        {
            UInt64 step_uint = assert_cast<const ColumnConst &>(*column_ptrs[2]).getUInt(0);
            Int64 step_int = assert_cast<const ColumnConst &>(*column_ptrs[2]).getInt(0);
            Float32 step_float32 = assert_cast<const ColumnConst &>(*column_ptrs[2]).getFloat32(0);
            Float64 step_float64 = assert_cast<const ColumnConst &>(*column_ptrs[2]).getFloat64(0);

            if ((res = executeConstStep<UInt8>(column_ptrs[0], column_ptrs[1], step_uint, input_rows_count))
                || (res = executeConstStep<UInt16>(column_ptrs[0], column_ptrs[1], step_uint, input_rows_count))
                || (res = executeConstStep<UInt64>(column_ptrs[0], column_ptrs[1], step_uint, input_rows_count))
                || (res = executeConstStep<UInt32>(column_ptrs[0], column_ptrs[1], static_cast<UInt32>(step_uint), input_rows_count))
                || (res = executeConstStep<Int8>(column_ptrs[0], column_ptrs[1], step_int, input_rows_count))
                || (res = executeConstStep<Int16>(column_ptrs[0], column_ptrs[1], step_int, input_rows_count))
                || (res = executeConstStep<Int32>(column_ptrs[0], column_ptrs[1], static_cast<Int32>(step_uint), input_rows_count))
                || (res = executeConstStep<Int64>(column_ptrs[0], column_ptrs[1], step_int, input_rows_count))
                || (res = executeConstStep<Float32>(column_ptrs[0], column_ptrs[1], step_float32, input_rows_count))
                || (res = executeConstStep<Float64>(column_ptrs[0], column_ptrs[1], step_float64, input_rows_count)))
            {
            }
        }
        else
        {
            if ((res = executeGeneric<UInt8>(column_ptrs[0], column_ptrs[1], column_ptrs[2], input_rows_count))
                || (res = executeGeneric<UInt16>(column_ptrs[0], column_ptrs[1], column_ptrs[2], input_rows_count))
                || (res = executeGeneric<UInt32>(column_ptrs[0], column_ptrs[1], column_ptrs[2], input_rows_count))
                || (res = executeGeneric<UInt64>(column_ptrs[0], column_ptrs[1], column_ptrs[2], input_rows_count))
                || (res = executeGeneric<Int8>(column_ptrs[0], column_ptrs[1], column_ptrs[2], input_rows_count))
                || (res = executeGeneric<Int16>(column_ptrs[0], column_ptrs[1], column_ptrs[2], input_rows_count))
                || (res = executeGeneric<Int32>(column_ptrs[0], column_ptrs[1], column_ptrs[2], input_rows_count))
                || (res = executeGeneric<Float32>(column_ptrs[0], column_ptrs[1], column_ptrs[2], input_rows_count))
                || (res = executeGeneric<Float64>(column_ptrs[0], column_ptrs[1], column_ptrs[2], input_rows_count))
                || (res = executeGeneric<Int64>(column_ptrs[0], column_ptrs[1], column_ptrs[2], input_rows_count)))
            {
            }
        }

        if (!res)
        {
            throw Exception
            {
                "Illegal columns " + column_ptrs[0]->getName() + " of argument of function " + getName(), ErrorCodes::ILLEGAL_COLUMN
            };
        }

        return res;
    }

    template <typename T>
    ColumnPtr executeConstStartStep(const IColumn * end_arg, const T start, const T step, const size_t input_rows_count) const
    {
        auto end_column = checkAndGetColumn<ColumnVector<T>>(end_arg);
        if (!end_column)
            return nullptr;

        const auto & end_data = end_column->getData();

        //The maximum number of values in KQL is 1,048,576 (2^20).
        size_t total_elements = max_elements < 1048576 ? max_elements : 1048576;
        if (total_elements > max_array_size_as_field)
            total_elements = max_array_size_as_field;

        size_t total_values = 0;
        size_t pre_values = 0;

        for (size_t row_idx = 0; row_idx < input_rows_count; ++row_idx)
        {
            if (start < end_data[row_idx] && step == 0)
                throw Exception
                {
                    "A call to function " + getName() + " overflows, the 3rd argument step can't be zero",
                    ErrorCodes::ARGUMENT_OUT_OF_BOUND
                };

            if (step > 0 && start <= end_data[row_idx])
            {
                pre_values += start >= end_data[row_idx] ? 0
                                : static_cast<size_t>((end_data[row_idx] - start) / (step) + 1);
            }

            if (step < 0 && start >= end_data[row_idx])
            {
                pre_values += start <= end_data[row_idx] ? 0
                                :  static_cast<size_t>((start - end_data[row_idx]) / (-step) + 1);
            }

            if (pre_values < total_values)
                throw Exception
                {
                    "A call to function " + getName() + " overflows, investigate the values of arguments you are passing",
                    ErrorCodes::ARGUMENT_OUT_OF_BOUND
                };

            total_values = pre_values;
            if (total_values > total_elements)
                total_values = total_elements;
        }

        auto data_col = ColumnVector<T>::create(total_values);
        auto offsets_col = ColumnArray::ColumnOffsets::create(end_column->size());

        auto & out_data = data_col->getData();
        auto & out_offsets = offsets_col->getData();

        IColumn::Offset offset{};
        for (size_t row_idx = 0; row_idx < input_rows_count; ++row_idx)
        {
            if (step > 0)
            {
                T st = start;
                T ed = end_data[row_idx];
                while (st <= ed)
                {
                    out_data[offset++] = st;
                    if (offset >= total_values)
                        break;
                    if (st > st + step)
                        throw Exception
                        {
                            "A call to function " + getName() + " overflows, investigate the values of arguments you are passing",
                            ErrorCodes::ARGUMENT_OUT_OF_BOUND
                        };
                    st += step;
                }
            }
            else
            {
                T st = start;
                T ed = end_data[row_idx];
                while (st >= ed)
                {
                    out_data[offset++] = st;
                    if (offset >= total_values)
                        break;
                    if (st < st + step)
                        throw Exception
                        {
                            "A call to function " + getName() + " overflows, investigate the values of arguments you are passing",
                            ErrorCodes::ARGUMENT_OUT_OF_BOUND
                        };
                    st += step;
                }
            }
            out_offsets[row_idx] = offset;
        }

        return ColumnArray::create(std::move(data_col), std::move(offsets_col));
    }

    template <typename T>
    ColumnPtr executeConstStep(const IColumn * start_arg, const IColumn * end_arg, const T step, const size_t input_rows_count) const
    {
        auto start_column = checkAndGetColumn<ColumnVector<T>>(start_arg);
        auto end_column = checkAndGetColumn<ColumnVector<T>>(end_arg);
        if (!end_column || !start_column)
            return nullptr;

        const auto & start_data = start_column->getData();
        const auto & end_data = end_column->getData();

        //The maximum number of values in KQL is 1,048,576 (2^20).
        size_t total_elements = max_elements < 1048576 ? max_elements : 1048576;
        if (total_elements > max_array_size_as_field)
            total_elements = max_array_size_as_field;

        size_t total_values = 0;
        size_t pre_values = 0;

        for (size_t row_idx = 0; row_idx < input_rows_count; ++row_idx)
        {
            if (start_data[row_idx] < end_data[row_idx] && step == 0)
                throw Exception
                {
                    "A call to function " + getName() + " overflows, the 3rd argument step can't be zero",
                    ErrorCodes::ARGUMENT_OUT_OF_BOUND
                };

            if (step > 0 && start_data[row_idx] <= end_data[row_idx])
            {
                pre_values += start_data[row_idx] >= end_data[row_idx] ? 0
                                : static_cast<size_t>((end_data[row_idx] - start_data[row_idx]) / (step) + 1);
            }

            if (step < 0 && start_data[row_idx] >= end_data[row_idx])
            {
                pre_values += start_data[row_idx] <= end_data[row_idx] ? 0
                                :  static_cast<size_t>((start_data[row_idx] - end_data[row_idx]) / (-step) + 1);
            }

            if (pre_values < total_values)
                throw Exception
                {
                    "A call to function " + getName() + " overflows, investigate the values of arguments you are passing",
                    ErrorCodes::ARGUMENT_OUT_OF_BOUND
                };

            total_values = pre_values;
            if (total_values > total_elements)
                total_values = total_elements;
        }

        auto data_col = ColumnVector<T>::create(total_values);
        auto offsets_col = ColumnArray::ColumnOffsets::create(end_column->size());

        auto & out_data = data_col->getData();
        auto & out_offsets = offsets_col->getData();

        IColumn::Offset offset{};
        for (size_t row_idx = 0; row_idx < input_rows_count; ++row_idx)
        {
            if (step > 0)
            {
                T st = start_data[row_idx];
                T ed = end_data[row_idx];
                while (st <= ed)
                {
                    out_data[offset++] = st;
                    if (offset >= total_values)
                        break;
                    if (st > st + step)
                        throw Exception
                        {
                            "A call to function " + getName() + " overflows, investigate the values of arguments you are passing",
                            ErrorCodes::ARGUMENT_OUT_OF_BOUND
                        };
                    st += step;
                }
            }
            else
            {
                T st = start_data[row_idx];
                T ed = end_data[row_idx];
                while (st >= ed)
                {
                    out_data[offset++] = st;
                    if (offset >= total_values)
                        break;
                    if (st < st + step)
                        throw Exception
                        {
                            "A call to function " + getName() + " overflows, investigate the values of arguments you are passing",
                            ErrorCodes::ARGUMENT_OUT_OF_BOUND
                        };
                    st += step;
                }
            }
            out_offsets[row_idx] = offset;
        }

        return ColumnArray::create(std::move(data_col), std::move(offsets_col));
    }

    template <typename T>
    ColumnPtr executeConstStart(const IColumn * end_arg, const IColumn * step_arg, const T start, const size_t input_rows_count) const
    {
        auto end_column = checkAndGetColumn<ColumnVector<T>>(end_arg);
        auto step_column = checkAndGetColumn<ColumnVector<T>>(step_arg);
        if (!end_column || !step_column)
            return nullptr;

        const auto & end_data = end_column->getData();
        const auto & step_data = step_column->getData();

        //The maximum number of values in KQL is 1,048,576 (2^20).
        size_t total_elements = max_elements < 1048576 ? max_elements : 1048576;
        if (total_elements > max_array_size_as_field)
            total_elements = max_array_size_as_field;

        size_t total_values = 0;
        size_t pre_values = 0;

        for (size_t row_idx = 0; row_idx < input_rows_count; ++row_idx)
        {
            if (start < end_data[row_idx] && step_data[row_idx] == 0)
                throw Exception
                {
                    "A call to function " + getName() + " overflows, the 3rd argument step can't be zero",
                    ErrorCodes::ARGUMENT_OUT_OF_BOUND
                };

            if (step_data[row_idx] > 0 && start <= end_data[row_idx])
            {
                pre_values += start >= end_data[row_idx] ? 0
                                : static_cast<size_t>((end_data[row_idx] - start) / (step_data[row_idx]) + 1);
            }

            if (step_data[row_idx] < 0 && start >= end_data[row_idx])
            {
                pre_values += start <= end_data[row_idx] ? 0
                                :  static_cast<size_t>((start - end_data[row_idx]) / (-step_data[row_idx]) + 1);
            }

            if (pre_values < total_values)
                throw Exception
                {
                    "A call to function " + getName() + " overflows, investigate the values of arguments you are passing",
                    ErrorCodes::ARGUMENT_OUT_OF_BOUND
                };

            total_values = pre_values;
            if (total_values > total_elements)
                total_values = total_elements;
        }

        auto data_col = ColumnVector<T>::create(total_values);
        auto offsets_col = ColumnArray::ColumnOffsets::create(end_column->size());

        auto & out_data = data_col->getData();
        auto & out_offsets = offsets_col->getData();

        IColumn::Offset offset{};
        for (size_t row_idx = 0; row_idx < input_rows_count; ++row_idx)
        {
            if (step_data[row_idx] > 0)
            {
                T st = start;
                T ed = end_data[row_idx];
                while (st <= ed)
                {
                    out_data[offset++] = st;
                    if (offset >= total_values)
                        break;
                    if (st > st + step_data[row_idx])
                        throw Exception
                        {
                            "A call to function " + getName() + " overflows, investigate the values of arguments you are passing",
                            ErrorCodes::ARGUMENT_OUT_OF_BOUND
                        };
                    st += step_data[row_idx];
                }
            }
            else
            {
                T st = start;
                T ed = end_data[row_idx];
                while (st >= ed)
                {
                    out_data[offset++] = st;
                    if (offset >= total_values)
                        break;
                    if (st < st + step_data[row_idx])
                        throw Exception
                        {
                            "A call to function " + getName() + " overflows, investigate the values of arguments you are passing",
                            ErrorCodes::ARGUMENT_OUT_OF_BOUND
                        };
                    st += step_data[row_idx];
                }
            }
            out_offsets[row_idx] = offset;
        }

        return ColumnArray::create(std::move(data_col), std::move(offsets_col));
    }

    template <typename T>
    ColumnPtr
    executeGeneric(const IColumn * start_col, const IColumn * end_col, const IColumn * step_col, const size_t input_rows_count) const
    {
        auto start_column = checkAndGetColumn<ColumnVector<T>>(start_col);
        auto end_column = checkAndGetColumn<ColumnVector<T>>(end_col);
        auto step_column = checkAndGetColumn<ColumnVector<T>>(step_col);

        if (!start_column || !end_column || !step_column)
            return nullptr;

        //The maximum number of values in KQL is 1,048,576 (2^20).
        size_t total_elements = max_elements < 1048576 ? max_elements : 1048576;
        if (total_elements > max_array_size_as_field)
            total_elements = max_array_size_as_field;

        const auto & start_data = start_column->getData();
        const auto & end_start = end_column->getData();
        const auto & step_data = step_column->getData();

        size_t total_values = 0;
        size_t pre_values = 0;

        for (size_t row_idx = 0; row_idx < input_rows_count; ++row_idx)
        {
            if (start_data[row_idx] < end_start[row_idx] && step_data[row_idx] == 0)
                throw Exception
                {
                    "A call to function " + getName() + " overflows, the 3rd argument step can't be zero",
                    ErrorCodes::ARGUMENT_OUT_OF_BOUND
                };

            if (step_data[row_idx] > 0 && start_data[row_idx] <= end_start[row_idx])
            {
                pre_values += start_data[row_idx] >= end_start[row_idx] ? 0
                                : static_cast<size_t>((end_start[row_idx] - start_data[row_idx]) / (step_data[row_idx]) + 1);
            }

            if (step_data[row_idx] < 0 && start_data[row_idx] >= end_start[row_idx])
            {
                pre_values += start_data[row_idx] <= end_start[row_idx] ? 0
                                :  static_cast<size_t>((start_data[row_idx] - end_start[row_idx]) / (-step_data[row_idx]) + 1);
            }

            if (pre_values < total_values)
                throw Exception
                {
                    "A call to function " + getName() + " overflows, investigate the values of arguments you are passing",
                    ErrorCodes::ARGUMENT_OUT_OF_BOUND
                };

            total_values = pre_values;
            if (total_values > total_elements)
                total_values = total_elements;
        }

        auto data_col = ColumnVector<T>::create(total_values);
        auto offsets_col = ColumnArray::ColumnOffsets::create(end_column->size());

        auto & out_data = data_col->getData();
        auto & out_offsets = offsets_col->getData();

        IColumn::Offset offset{};
        for (size_t row_idx = 0; row_idx < input_rows_count; ++row_idx)
        {
            if (step_data[row_idx] > 0)
            {
                auto st = start_data[row_idx];
                auto ed = end_start[row_idx];
                while (st <= ed)
                {
                    out_data[offset++] = st;
                    if (offset >= total_values)
                        break;
                    if (st > st + step_data[row_idx])
                        throw Exception
                        {
                            "A call to function " + getName() + " overflows, investigate the values of arguments you are passing",
                            ErrorCodes::ARGUMENT_OUT_OF_BOUND
                        };
                    st += step_data[row_idx];
                }
            }
            else
            {
                auto st = start_data[row_idx];
                auto ed = end_start[row_idx];
                while (st >= ed)
                {
                    out_data[offset++] = st;
                    if (offset >= total_values)
                        break;
                    if (st < st + step_data[row_idx])
                        throw Exception
                        {
                            "A call to function " + getName() + " overflows, investigate the values of arguments you are passing",
                            ErrorCodes::ARGUMENT_OUT_OF_BOUND
                        };
                    st += step_data[row_idx];
                }
            }
            out_offsets[row_idx] = offset;
        }

        return ColumnArray::create(std::move(data_col), std::move(offsets_col));
    }

    ColumnPtr executeDateTime64(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const
    {
        DataTypePtr elem_type = checkAndGetDataType<DataTypeArray>(result_type.get())->getNestedType();
        const auto & start_col = arguments[0].column;
        const auto & end_col = arguments[1].column;
        Int64 step_value = 3600000000000;

        const auto & start_data = typeid_cast<const ColumnDecimal<DateTime64> &>(*start_col).getData();
        const auto & end_data = typeid_cast<const ColumnDecimal<DateTime64> &>(*end_col).getData();

        //The maximum number of values in KQL is 1,048,576 (2^20).
        size_t total_elements = max_elements < 1048576 ? max_elements : 1048576;
        if (total_elements > max_array_size_as_field)
            total_elements = max_array_size_as_field;

        size_t total_values = 0;
        size_t pre_values = 0;
        for (size_t row_idx = 0; row_idx < input_rows_count; ++row_idx)
        {
            if (arguments.size() > 2)
                step_value = arguments[2].column->getInt(row_idx);

            if (start_data[row_idx] < end_data[row_idx] && step_value == 0)
                throw Exception
                {
                    "A call to function " + getName() + " overflows, the 3rd argument step can't be zero",
                    ErrorCodes::ARGUMENT_OUT_OF_BOUND
                };

            if (step_value > 0 && start_data[row_idx] <= end_data[row_idx])
            {
                pre_values += start_data[row_idx] >= end_data[row_idx] ? 0
                                : static_cast<size_t>((end_data[row_idx] - start_data[row_idx]) / (step_value) + 1);
            }

            if (step_value < 0 && start_data[row_idx] >= end_data[row_idx])
            {
                pre_values += start_data[row_idx] <= end_data[row_idx] ? 0
                                :  static_cast<size_t>((start_data[row_idx] - end_data[row_idx]) / (-step_value) + 1);
            }

            if (pre_values < total_values)
                throw Exception
                {
                    "A call to function " + getName() + " overflows, investigate the values of arguments you are passing",
                    ErrorCodes::ARGUMENT_OUT_OF_BOUND
                };

            total_values = pre_values;
            if (total_values > total_elements)
                total_values = total_elements;
        }

        auto data_col = ColumnDecimal<DateTime64>::create(total_values, 9);
        auto offsets_col = ColumnArray::ColumnOffsets::create(end_col->size());

        auto & out_data = data_col->getData();
        auto & out_offsets = offsets_col->getData();
        IColumn::Offset offset{};
        for (size_t row_idx = 0; row_idx < input_rows_count; ++row_idx)
        {
            if (arguments.size() > 2)
                step_value = arguments[2].column->getInt(row_idx);
            if (step_value > 0)
            {
                for (size_t st = start_data[row_idx], ed = end_data[row_idx]; st <= ed; st += step_value)
                {
                    out_data[offset++] = st;
                    if (offset >= total_values)
                        break;
                    if (st > st + step_value)
                        throw Exception
                        {
                            "A call to function " + getName() + " overflows, investigate the values of arguments you are passing",
                            ErrorCodes::ARGUMENT_OUT_OF_BOUND
                        };
                }
            }
            else
            {
                for (size_t st = start_data[row_idx], ed = end_data[row_idx]; st >= ed; st += step_value)
                {
                    out_data[offset++] = st;
                    if (offset >= total_values)
                        break;
                    if (st < st + step_value)
                        throw Exception
                        {
                            "A call to function " + getName() + " overflows, investigate the values of arguments you are passing",
                            ErrorCodes::ARGUMENT_OUT_OF_BOUND
                        };
                }
            }
            out_offsets[row_idx] = offset;
        }
        return ColumnArray::create(std::move(data_col), std::move(offsets_col));
    }

    ColumnPtr executeInterval(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const
    {
        DataTypePtr elem_type = checkAndGetDataType<DataTypeArray>(result_type.get())->getNestedType();
        const auto & start_col = arguments[0].column;
        const auto & end_col = arguments[1].column;
        Int64 step_value = 3600000000000;

        const auto & start_data = typeid_cast<const ColumnVector<Int64> &>(*start_col).getData();
        const auto & end_data = typeid_cast<const ColumnVector<Int64> &>(*end_col).getData();

        //The maximum number of values in KQL is 1,048,576 (2^20).
        size_t total_elements = max_elements < 1048576 ? max_elements : 1048576;
        if (total_elements > max_array_size_as_field)
            total_elements = max_array_size_as_field;

        size_t total_values = 0;
        size_t pre_values = 0;
        for (size_t row_idx = 0; row_idx < input_rows_count; ++row_idx)
        {
            if (arguments.size() > 2)
                step_value = arguments[2].column->getInt(row_idx);

            if (start_data[row_idx] < end_data[row_idx] && step_value == 0)
                throw Exception
                {
                    "A call to function " + getName() + " overflows, the 3rd argument step can't be zero",
                    ErrorCodes::ARGUMENT_OUT_OF_BOUND
                };

            if (step_value > 0 && start_data[row_idx] <= end_data[row_idx])
            {
                pre_values += start_data[row_idx] >= end_data[row_idx] ? 0
                                : static_cast<size_t>((end_data[row_idx] - start_data[row_idx]) / (step_value) + 1);
            }

            if (step_value < 0 && start_data[row_idx] >= end_data[row_idx])
            {
                pre_values += start_data[row_idx] <= end_data[row_idx] ? 0
                                :  static_cast<size_t>((start_data[row_idx] - end_data[row_idx]) / (-step_value) + 1);
            }
            if (pre_values < total_values)
                throw Exception
                {
                    "A call to function " + getName() + " overflows, investigate the values of arguments you are passing",
                    ErrorCodes::ARGUMENT_OUT_OF_BOUND
                };

            total_values = pre_values;
            if (total_values > total_elements)
                total_values = total_elements;
        }

        auto out = ColumnArray::create(std::make_shared<DataTypeInterval>(IntervalKind::Nanosecond)->createColumn());
        IColumn & out_data = out->getData();
        IColumn::Offsets & out_offsets = out->getOffsets();

        out_data.reserve(input_rows_count * total_values);
        out_offsets.resize(input_rows_count);
        IColumn::Offset current_offset = 0;
        for (size_t row_idx = 0; row_idx < input_rows_count; ++row_idx)
        {
            if (arguments.size() > 2)
                step_value = arguments[2].column->getInt(row_idx);
            if (step_value > 0)
            {
                for (size_t st = start_data[row_idx], ed = end_data[row_idx]; st <= ed; st += step_value)
                {
                    out_data.insert(Field(st));
                    current_offset++;
                    if (current_offset >= total_values)
                        break;
                    if (st > st + step_value)
                        throw Exception
                        {
                            "A call to function " + getName() + " overflows, investigate the values of arguments you are passing",
                            ErrorCodes::ARGUMENT_OUT_OF_BOUND
                        };
                }
            }
            else
            {
                for (size_t st = start_data[row_idx], ed = end_data[row_idx]; st >= ed; st += step_value)
                {
                    out_data.insert(Field(st));
                    current_offset++;
                    if (current_offset >= total_values)
                        break;
                    if (st < st + step_value)
                        throw Exception
                        {
                            "A call to function " + getName() + " overflows, investigate the values of arguments you are passing",
                            ErrorCodes::ARGUMENT_OUT_OF_BOUND
                        };
                }
            }
            out_offsets[row_idx] = current_offset;
        }

        return out;
    }
};

REGISTER_FUNCTION(KqlRange)
{
    factory.registerFunction<FunctionKqlRange>();
}

}
