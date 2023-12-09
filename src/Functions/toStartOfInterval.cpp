#include <base/arithmeticOverflow.h>
#include <Common/DateLUTImpl.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnsDateTime.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDate32.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeInterval.h>
#include <Functions/DateTimeTransforms.h>
#include <Functions/FunctionFactory.h>
#include <Functions/IFunction.h>
#include <IO/WriteHelpers.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int ILLEGAL_COLUMN;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int ARGUMENT_OUT_OF_BOUND;
    extern const int DECIMAL_OVERFLOW;
    extern const int LOGICAL_ERROR;
}


namespace
{
    enum class ExecutionErrorPolicy
    {
        Null,
        Throw
    };

    template <IntervalKind::Kind unit>
    struct Transform;

    template <>
    struct Transform<IntervalKind::Year>
    {
        static UInt16 execute(UInt16 d, Int64 years, const DateLUTImpl & time_zone, Int64, const char*)
        {
            return time_zone.toStartOfYearInterval(DayNum(d), years);
        }

        static UInt16 execute(Int32 d, Int64 years, const DateLUTImpl & time_zone, Int64, const char*)
        {
            return time_zone.toStartOfYearInterval(ExtendedDayNum(d), years);
        }

        static UInt16 execute(UInt32 t, Int64 years, const DateLUTImpl & time_zone, Int64, const char*)
        {
            return time_zone.toStartOfYearInterval(time_zone.toDayNum(t), years);
        }

        static UInt16 execute(Int64 t, Int64 years, const DateLUTImpl & time_zone, Int64 scale_multiplier, const char*)
        {
            return time_zone.toStartOfYearInterval(time_zone.toDayNum(t / scale_multiplier), years);
        }
    };

    template <>
    struct Transform<IntervalKind::Quarter>
    {
        static UInt16 execute(UInt16 d, Int64 quarters, const DateLUTImpl & time_zone, Int64, const char*)
        {
            return time_zone.toStartOfQuarterInterval(DayNum(d), quarters);
        }

        static UInt16 execute(Int32 d, Int64 quarters, const DateLUTImpl & time_zone, Int64, const char*)
        {
            return time_zone.toStartOfQuarterInterval(ExtendedDayNum(d), quarters);
        }

        static UInt16 execute(UInt32 t, Int64 quarters, const DateLUTImpl & time_zone, Int64, const char*)
        {
            return time_zone.toStartOfQuarterInterval(time_zone.toDayNum(t), quarters);
        }

        static UInt16 execute(Int64 t, Int64 quarters, const DateLUTImpl & time_zone, Int64 scale_multiplier, const char*)
        {
            return time_zone.toStartOfQuarterInterval(time_zone.toDayNum(t / scale_multiplier), quarters);
        }
    };

    template <>
    struct Transform<IntervalKind::Month>
    {
        static UInt16 execute(UInt16 d, Int64 months, const DateLUTImpl & time_zone, Int64, const char*)
        {
            return time_zone.toStartOfMonthInterval(DayNum(d), months);
        }

        static UInt16 execute(Int32 d, Int64 months, const DateLUTImpl & time_zone, Int64, const char*)
        {
            return time_zone.toStartOfMonthInterval(ExtendedDayNum(d), months);
        }

        static UInt16 execute(UInt32 t, Int64 months, const DateLUTImpl & time_zone, Int64, const char*)
        {
            return time_zone.toStartOfMonthInterval(time_zone.toDayNum(t), months);
        }

        static UInt16 execute(Int64 t, Int64 months, const DateLUTImpl & time_zone, Int64 scale_multiplier, const char*)
        {
            return time_zone.toStartOfMonthInterval(time_zone.toDayNum(t / scale_multiplier), months);
        }
    };

    template <>
    struct Transform<IntervalKind::Week>
    {
        static UInt16 execute(UInt16 d, Int64 weeks, const DateLUTImpl & time_zone, Int64, const char*)
        {
            return time_zone.toStartOfWeekInterval(DayNum(d), weeks);
        }

        static UInt16 execute(Int32 d, Int64 weeks, const DateLUTImpl & time_zone, Int64, const char*)
        {
            return time_zone.toStartOfWeekInterval(ExtendedDayNum(d), weeks);
        }

        static UInt16 execute(UInt32 t, Int64 weeks, const DateLUTImpl & time_zone, Int64, const char*)
        {
            return time_zone.toStartOfWeekInterval(time_zone.toDayNum(t), weeks);
        }

        static UInt16 execute(Int64 t, Int64 weeks, const DateLUTImpl & time_zone, Int64 scale_multiplier, const char*)
        {
            return time_zone.toStartOfWeekInterval(time_zone.toDayNum(t / scale_multiplier), weeks);
        }
    };

    template <>
    struct Transform<IntervalKind::Day>
    {
        static UInt32 execute(UInt16 d, Int64 days, const DateLUTImpl & time_zone, Int64, const char*)
        {
            return static_cast<UInt32>(time_zone.toStartOfDayInterval(ExtendedDayNum(d), days));
        }

        static UInt32 execute(Int32 d, Int64 days, const DateLUTImpl & time_zone, Int64, const char*)
        {
            return static_cast<UInt32>(time_zone.toStartOfDayInterval(ExtendedDayNum(d), days));
        }

        static UInt32 execute(UInt32 t, Int64 days, const DateLUTImpl & time_zone, Int64, const char*)
        {
            return static_cast<UInt32>(time_zone.toStartOfDayInterval(time_zone.toDayNum(t), days));
        }

        static Int64 execute(Int64 t, Int64 days, const DateLUTImpl & time_zone, Int64 scale_multiplier, const char*)
        {
            return time_zone.toStartOfDayInterval(time_zone.toDayNum(t / scale_multiplier), days);
        }
    };

    template <>
    struct Transform<IntervalKind::Hour>
    {
        static UInt32 execute(UInt16, Int64, const DateLUTImpl &, Int64, const char* function_name) { throwDateIsNotSupported(function_name); }

        static UInt32 execute(Int32, Int64, const DateLUTImpl &, Int64, const char* function_name) { throwDateIsNotSupported(function_name); }

        static UInt32 execute(UInt32 t, Int64 hours, const DateLUTImpl & time_zone, Int64, const char*)
        {
            return time_zone.toStartOfHourInterval(t, hours);
        }

        static Int64 execute(Int64 t, Int64 hours, const DateLUTImpl & time_zone, Int64 scale_multiplier, const char*)
        {
            return time_zone.toStartOfHourInterval(t / scale_multiplier, hours);
        }
    };

    template <>
    struct Transform<IntervalKind::Minute>
    {
        static UInt32 execute(UInt16, Int64, const DateLUTImpl &, Int64, const char* function_name) { throwDateIsNotSupported(function_name); }

        static UInt32 execute(Int32, Int64, const DateLUTImpl &, Int64, const char* function_name) { throwDateIsNotSupported(function_name); }

        static UInt32 execute(UInt32 t, Int64 minutes, const DateLUTImpl & time_zone, Int64, const char*)
        {
            return time_zone.toStartOfMinuteInterval(t, minutes);
        }

        static Int64 execute(Int64 t, Int64 minutes, const DateLUTImpl & time_zone, Int64 scale_multiplier, const char*)
        {
            return time_zone.toStartOfMinuteInterval(t / scale_multiplier, minutes);
        }
    };

    template <>
    struct Transform<IntervalKind::Second>
    {
        static UInt32 execute(UInt16, Int64, const DateLUTImpl &, Int64, const char* function_name) { throwDateIsNotSupported(function_name); }

        static UInt32 execute(Int32, Int64, const DateLUTImpl &, Int64, const char* function_name) { throwDateIsNotSupported(function_name); }

        static UInt32 execute(UInt32 t, Int64 seconds, const DateLUTImpl & time_zone, Int64, const char*)
        {
            return time_zone.toStartOfSecondInterval(t, seconds);
        }

        static Int64 execute(Int64 t, Int64 seconds, const DateLUTImpl & time_zone, Int64 scale_multiplier, const char*)
        {
            return time_zone.toStartOfSecondInterval(t / scale_multiplier, seconds);
        }
    };

    template <>
    struct Transform<IntervalKind::Millisecond>
    {
        static UInt32 execute(UInt16, Int64, const DateLUTImpl &, Int64, const char* function_name) { throwDateIsNotSupported(function_name); }

        static UInt32 execute(Int32, Int64, const DateLUTImpl &, Int64, const char* function_name) { throwDateIsNotSupported(function_name); }

        static UInt32 execute(UInt32, Int64, const DateLUTImpl &, Int64, const char* function_name) { throwDateTimeIsNotSupported(function_name); }

        static Int64 execute(Int64 t, Int64 milliseconds, const DateLUTImpl &, Int64 scale_multiplier, const char*)
        {
            if (scale_multiplier < 1000)
            {
                Int64 t_milliseconds = 0;
                if (common::mulOverflow(t, static_cast<Int64>(1000) / scale_multiplier, t_milliseconds))
                    throw DB::Exception(ErrorCodes::DECIMAL_OVERFLOW, "Numeric overflow");
                if (likely(t >= 0))
                    return t_milliseconds / milliseconds * milliseconds;
                else
                    return ((t_milliseconds + 1) / milliseconds - 1) * milliseconds;
            }
            else if (scale_multiplier > 1000)
            {
                Int64 scale_diff = scale_multiplier / static_cast<Int64>(1000);
                if (likely(t >= 0))
                    return t / milliseconds / scale_diff * milliseconds;
                else
                    return ((t + 1) / milliseconds / scale_diff - 1) * milliseconds;
            }
            else
                if (likely(t >= 0))
                    return t / milliseconds * milliseconds;
                else
                    return ((t + 1) / milliseconds - 1) * milliseconds;
        }
    };

    template <>
    struct Transform<IntervalKind::Microsecond>
    {
        static UInt32 execute(UInt16, Int64, const DateLUTImpl &, Int64, const char* function_name) { throwDateIsNotSupported(function_name); }

        static UInt32 execute(Int32, Int64, const DateLUTImpl &, Int64, const char* function_name) { throwDateIsNotSupported(function_name); }

        static UInt32 execute(UInt32, Int64, const DateLUTImpl &, Int64, const char* function_name) { throwDateTimeIsNotSupported(function_name); }

        static Int64 execute(Int64 t, Int64 microseconds, const DateLUTImpl &, Int64 scale_multiplier, const char*)
        {
            if (scale_multiplier < 1000000)
            {
                Int64 t_microseconds = 0;
                if (common::mulOverflow(t, static_cast<Int64>(1000000) / scale_multiplier, t_microseconds))
                    throw DB::Exception(ErrorCodes::DECIMAL_OVERFLOW, "Numeric overflow");
                if (likely(t >= 0))
                    return t_microseconds / microseconds * microseconds;
                else
                    return ((t_microseconds + 1) / microseconds - 1) * microseconds;
            }
            else if (scale_multiplier > 1000000)
            {
                Int64 scale_diff = scale_multiplier / static_cast<Int64>(1000000);
                if (likely(t >= 0))
                    return t / microseconds / scale_diff * microseconds;
                else
                    return ((t + 1) / microseconds / scale_diff - 1) * microseconds;
            }
            else
                if (likely(t >= 0))
                    return t / microseconds * microseconds;
                else
                    return ((t + 1) / microseconds - 1) * microseconds;
        }
    };

    template <>
    struct Transform<IntervalKind::Nanosecond>
    {
        static UInt32 execute(UInt16, Int64, const DateLUTImpl &, Int64, const char* function_name) { throwDateIsNotSupported(function_name); }

        static UInt32 execute(Int32, Int64, const DateLUTImpl &, Int64, const char* function_name) { throwDateIsNotSupported(function_name); }

        static UInt32 execute(UInt32, Int64, const DateLUTImpl &, Int64, const char* function_name) { throwDateTimeIsNotSupported(function_name); }

        static Int64 execute(Int64 t, Int64 nanoseconds, const DateLUTImpl &, Int64 scale_multiplier, const char*)
        {
            if (scale_multiplier < 1000000000)
            {
                Int64 t_nanoseconds = 0;
                if (common::mulOverflow(t, (static_cast<Int64>(1000000000) / scale_multiplier), t_nanoseconds))
                    throw DB::Exception(ErrorCodes::DECIMAL_OVERFLOW, "Numeric overflow");
                if (likely(t >= 0))
                    return t_nanoseconds / nanoseconds * nanoseconds;
                else
                    return ((t_nanoseconds + 1) / nanoseconds - 1) * nanoseconds;
            }
            else
                if (likely(t >= 0))
                    return t / nanoseconds * nanoseconds;
                else
                    return ((t + 1) / nanoseconds - 1) * nanoseconds;
        }
    };

template <ExecutionErrorPolicy execution_error_policy>
class FunctionToStartOfInterval : public IFunction
{
public:
    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionToStartOfInterval>(); }

    static constexpr auto name = std::invoke(
        []
        {
            if (execution_error_policy == ExecutionErrorPolicy::Null)
                return "toStartOfIntervalOrNull";
            else if (execution_error_policy == ExecutionErrorPolicy::Throw)
                return "toStartOfInterval";

            throw DB::Exception(ErrorCodes::LOGICAL_ERROR, "Unhandled execution policy");
        });

    String getName() const override { return name; }

    bool isVariadic() const override { return true; }
    size_t getNumberOfArguments() const override { return 0; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }

    bool useDefaultImplementationForConstants() const override { return true; }
    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {1, 2}; }

    bool hasInformationAboutMonotonicity() const override { return true; }
    Monotonicity getMonotonicityForRange(const IDataType &, const Field &, const Field &) const override
    {
        return { .is_monotonic = true, .is_always_monotonic = true };
    }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        bool first_argument_is_date = false;
        auto check_first_argument = [&]
        {
            if (!isDate(arguments[0].type) && !isDateTime(arguments[0].type) && !isDateTime64(arguments[0].type))
                throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Illegal type {} of argument of function {}. "
                    "Should be a date or a date with time", arguments[0].type->getName(), getName());
            first_argument_is_date = isDate(arguments[0].type);
        };

        const DataTypeInterval * interval_type = nullptr;
        bool result_type_is_date = false;
        bool result_type_is_datetime = false;
        auto check_interval_argument = [&]
        {
            interval_type = checkAndGetDataType<DataTypeInterval>(arguments[1].type.get());
            if (!interval_type)
                throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Illegal type {} of argument of function {}. "
                    "Should be an interval of time", arguments[1].type->getName(), getName());
            result_type_is_date = (interval_type->getKind() == IntervalKind::Year)
                || (interval_type->getKind() == IntervalKind::Quarter) || (interval_type->getKind() == IntervalKind::Month)
                || (interval_type->getKind() == IntervalKind::Week);
            result_type_is_datetime = (interval_type->getKind() == IntervalKind::Day) || (interval_type->getKind() == IntervalKind::Hour)
                || (interval_type->getKind() == IntervalKind::Minute) || (interval_type->getKind() == IntervalKind::Second);
        };

        auto check_timezone_argument = [&]
        {
            if (!WhichDataType(arguments[2].type).isString())
                throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Illegal type {} of argument of function {}. "
                    "This argument is optional and must be a constant string with timezone name",
                    arguments[2].type->getName(), getName());
            if (first_argument_is_date && result_type_is_date)
                throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                    "The timezone argument of function {} with interval type {} is allowed only when the 1st argument "
                    "has the type DateTime or DateTime64",
                        getName(), interval_type->getKind().toString());
        };

        if (arguments.size() == 2)
        {
            check_first_argument();
            check_interval_argument();
        }
        else if (arguments.size() == 3)
        {
            check_first_argument();
            check_interval_argument();
            check_timezone_argument();
        }
        else
        {
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "Number of arguments for function {} doesn't match: passed {}, should be 2 or 3",
                getName(), arguments.size());
        }

        auto return_type = std::invoke(
            [&arguments, &interval_type, &result_type_is_date, &result_type_is_datetime]() -> std::shared_ptr<IDataType>
            {
                if (result_type_is_date)
                    return std::make_shared<DataTypeDate>();
                else if (result_type_is_datetime)
                    return std::make_shared<DataTypeDateTime>(extractTimeZoneNameFromFunctionArguments(arguments, 2, 0, false));
                else
                {
                    auto scale = 0;

                    if (interval_type->getKind() == IntervalKind::Nanosecond)
                        scale = 9;
                    else if (interval_type->getKind() == IntervalKind::Microsecond)
                        scale = 6;
                    else if (interval_type->getKind() == IntervalKind::Millisecond)
                        scale = 3;

                    return std::make_shared<DataTypeDateTime64>(scale, extractTimeZoneNameFromFunctionArguments(arguments, 2, 0, false));
                }
            });

        if constexpr (execution_error_policy == ExecutionErrorPolicy::Null)
            return makeNullable(return_type);

        return return_type;
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, const size_t) const override
    {
        const auto & time_column = arguments[0];
        const auto & interval_column = arguments[1];
        const auto & time_zone = extractTimeZoneFromFunctionArguments(arguments, 2, 0);
        return dispatchForTimeColumn(time_column, interval_column, result_type, time_zone);
    }

private:
    ColumnPtr dispatchForTimeColumn(
        const ColumnWithTypeAndName & time_column, const ColumnWithTypeAndName & interval_column, const DataTypePtr & result_type, const DateLUTImpl & time_zone) const
    {
        const auto & from_datatype = *time_column.type.get();

        if (isDateTime64(from_datatype))
        {
            const auto * time_column_vec = checkAndGetColumn<ColumnDateTime64>(time_column.column.get());
            auto scale = assert_cast<const DataTypeDateTime64 &>(from_datatype).getScale();

            if (time_column_vec)
                return dispatchForIntervalColumn(assert_cast<const DataTypeDateTime64 &>(from_datatype), *time_column_vec, interval_column, result_type, time_zone, scale);
        }
        if (isDateTime(from_datatype))
        {
            const auto * time_column_vec = checkAndGetColumn<ColumnDateTime>(time_column.column.get());
            if (time_column_vec)
                return dispatchForIntervalColumn(assert_cast<const DataTypeDateTime &>(from_datatype), *time_column_vec, interval_column, result_type, time_zone);
        }
        if (isDate(from_datatype))
        {
            const auto * time_column_vec = checkAndGetColumn<ColumnDate>(time_column.column.get());
            if (time_column_vec)
                return dispatchForIntervalColumn(assert_cast<const DataTypeDate &>(from_datatype), *time_column_vec, interval_column, result_type, time_zone);
        }
        if (isDate32(from_datatype))
        {
            const auto * time_column_vec = checkAndGetColumn<ColumnDate32>(time_column.column.get());
            if (time_column_vec)
                return dispatchForIntervalColumn(assert_cast<const DataTypeDate32 &>(from_datatype), *time_column_vec, interval_column, result_type, time_zone);
        }
        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Illegal column for first argument of function {}. Must contain dates or dates with time", getName());
    }

    template <typename TimeColumnType, typename TimeDataType>
    ColumnPtr dispatchForIntervalColumn(
        const TimeDataType & time_data_type, const TimeColumnType & time_column, const ColumnWithTypeAndName & interval_column,
        const DataTypePtr & result_type, const DateLUTImpl & time_zone, const UInt16 scale = 1) const
    {
        const auto * interval_type = checkAndGetDataType<DataTypeInterval>(interval_column.type.get());
        if (!interval_type)
            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Illegal column for second argument of function {}, must be an interval of time.", getName());

        const auto * interval_column_const_int64 = checkAndGetColumnConst<ColumnInt64>(interval_column.column.get());
        if (!interval_column_const_int64)
            throw Exception(
                ErrorCodes::ILLEGAL_COLUMN,
                "Illegal column for second argument of function {}, must be a const interval of time.",
                getName());

        const auto num_units = interval_column_const_int64->getValue<Int64>();
        switch (interval_type->getKind())
        {
            case IntervalKind::Nanosecond:
                return execute<TimeDataType, DataTypeDateTime64, IntervalKind::Nanosecond>(time_data_type, time_column, num_units, result_type, time_zone, scale);
            case IntervalKind::Microsecond:
                return execute<TimeDataType, DataTypeDateTime64, IntervalKind::Microsecond>(time_data_type, time_column, num_units, result_type, time_zone, scale);
            case IntervalKind::Millisecond:
                return execute<TimeDataType, DataTypeDateTime64, IntervalKind::Millisecond>(time_data_type, time_column, num_units, result_type, time_zone, scale);
            case IntervalKind::Second:
                return execute<TimeDataType, DataTypeDateTime, IntervalKind::Second>(time_data_type, time_column, num_units, result_type, time_zone, scale);
            case IntervalKind::Minute:
                return execute<TimeDataType, DataTypeDateTime, IntervalKind::Minute>(time_data_type, time_column, num_units, result_type, time_zone, scale);
            case IntervalKind::Hour:
                return execute<TimeDataType, DataTypeDateTime, IntervalKind::Hour>(time_data_type, time_column, num_units, result_type, time_zone, scale);
            case IntervalKind::Day:
                return execute<TimeDataType, DataTypeDateTime, IntervalKind::Day>(time_data_type, time_column, num_units, result_type, time_zone, scale);
            case IntervalKind::Week:
                return execute<TimeDataType, DataTypeDate, IntervalKind::Week>(time_data_type, time_column, num_units, result_type, time_zone, scale);
            case IntervalKind::Month:
                return execute<TimeDataType, DataTypeDate, IntervalKind::Month>(time_data_type, time_column, num_units, result_type, time_zone, scale);
            case IntervalKind::Quarter:
                return execute<TimeDataType, DataTypeDate, IntervalKind::Quarter>(time_data_type, time_column, num_units, result_type, time_zone, scale);
            case IntervalKind::Year:
                return execute<TimeDataType, DataTypeDate, IntervalKind::Year>(time_data_type, time_column, num_units, result_type, time_zone, scale);
        }

        UNREACHABLE();
    }

    template <typename TimeDataType, typename ToDataType, IntervalKind::Kind unit, typename ColumnType>
    ColumnPtr execute(const TimeDataType &, const ColumnType & time_column_type, Int64 num_units, const DataTypePtr & result_type, const DateLUTImpl & time_zone, const UInt16 scale) const
    {
        using ToColumnType = typename ToDataType::ColumnType;
        using ToFieldType = typename ToDataType::FieldType;

        const auto & time_data = time_column_type.getData();
        const auto size = time_data.size();

        auto result_col = result_type->createColumn();
        auto [result_null_map_data, result_value_data] = std::invoke(
            [&result_col]() -> std::pair<NullMap *, typename ToColumnType::Container &>
            {
                if constexpr (execution_error_policy == ExecutionErrorPolicy::Null)
                {
                    auto & nullable_column = assert_cast<ColumnNullable &>(*result_col);
                    auto & nested_column = assert_cast<ToColumnType &>(nullable_column.getNestedColumn());
                    return {&nullable_column.getNullMapData(), nested_column.getData()};
                }
                else if constexpr (execution_error_policy == ExecutionErrorPolicy::Throw)
                {
                    auto & target_column = assert_cast<ToColumnType &>(*result_col);
                    return {nullptr, target_column.getData()};
                }
            });

        if constexpr (execution_error_policy == ExecutionErrorPolicy::Null)
            result_null_map_data->resize(size, true);

        result_value_data.resize(size);
        if (num_units <= 0)
        {
            if constexpr (execution_error_policy == ExecutionErrorPolicy::Null)
                return result_col;
            else if constexpr (execution_error_policy == ExecutionErrorPolicy::Throw)
                throw Exception(ErrorCodes::ARGUMENT_OUT_OF_BOUND, "Value for second argument of function {} must be positive.", getName());
        }

        const auto scale_multiplier = DecimalUtils::scaleMultiplier<DateTime64>(scale);

        for (size_t i = 0; i != size; ++i)
        {
            try
            {
                result_value_data[i]
                    = static_cast<ToFieldType>(Transform<unit>::execute(time_data[i], num_units, time_zone, scale_multiplier, name));
                if constexpr (execution_error_policy == ExecutionErrorPolicy::Null)
                    (*result_null_map_data)[i] = false;
            }
            catch (...)
            {
                if constexpr (execution_error_policy == ExecutionErrorPolicy::Throw)
                    throw;
            }
        }

        return result_col;
    }
};

}

REGISTER_FUNCTION(ToStartOfInterval)
{
    factory.registerFunction<FunctionToStartOfInterval<ExecutionErrorPolicy::Null>>();
    factory.registerFunction<FunctionToStartOfInterval<ExecutionErrorPolicy::Throw>>();
}

}
