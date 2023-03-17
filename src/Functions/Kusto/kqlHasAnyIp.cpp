#include <optional>
#include <ranges>
#include <regex>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnDecimal.h>
#include <Columns/ColumnString.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>
#include <Functions/Kusto/KqlFunctionBase.h>
#include <boost/spirit/home/x3.hpp>
#include <format>

namespace DB
{
namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int BAD_ARGUMENTS;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}


struct HasAnyIpv4
{
    static constexpr auto name = "kql_has_any_ipv4";
    static constexpr auto variadic = true;
    static constexpr auto regex = "([^[:alnum:]]|^)([0-9]{1,3}\\.[0-9]{1,3}\\.[0-9]{1,3}\\.[0-9]{1,3})([^[:alnum:]]|$)";
    static std::string transformStringArgument(
        const ColumnWithTypeAndName & arg,
        [[maybe_unused]] const DataTypePtr & result_type,
        [[maybe_unused]] const ContextPtr & context,
        size_t row)
    {
        return arg.column->getDataAt(row).toString();
    }
    static bool
    checkStringArgument(const std::string & arg, const DataTypePtr & result_type, const ContextPtr & context, [[maybe_unused]] size_t row)
    {
        const auto is_ipv4_string = [&, result_type](const DB::ColumnsWithTypeAndName & args)
        { return DB::FunctionFactory::instance().get("isIPv4String", context)->build(args)->execute(args, result_type, 1); };

        const ColumnsWithTypeAndName is_ipv4_string_args = {createConstColumnWithTypeAndName<DataTypeString>(arg, "ip")};
        const auto is_ipv4 = is_ipv4_string(is_ipv4_string_args);
        return is_ipv4->getUInt(0) == 1;
    }

    static bool
    insertFromArrayElement(const Field & value, const DataTypePtr & result_type, const ContextPtr & context, std::vector<std::string> & ips)
    {
        const auto is_ipv4_string = [&, result_type](const DB::ColumnsWithTypeAndName & args)
        { return DB::FunctionFactory::instance().get("isIPv4String", context)->build(args)->execute(args, result_type, 1); };
        const auto value_as_string = toString(value);
        const ColumnsWithTypeAndName is_ipv4_string_args = {createConstColumnWithTypeAndName<DB::DataTypeString>(value_as_string, "ip")};
        const auto is_ipv4 = is_ipv4_string(is_ipv4_string_args);
        if (is_ipv4->getUInt(0) == 1)
        {
            ips.push_back(value_as_string);
            return true;
        }
        return false;
    }

    static bool checkRegexMatch(
        const std::string & s, const DataTypePtr & result_type, const ContextPtr & context, const std::vector<std::string> & ips)
    {
        const ColumnsWithTypeAndName is_ipv4_string_args = {createConstColumnWithTypeAndName<DataTypeString>(s, "ip")};

        const auto is_ipv4 = FunctionFactory::instance()
                                 .get("isIPv4String", context)
                                 ->build(is_ipv4_string_args)
                                 ->execute(is_ipv4_string_args, result_type, 1);

        if (is_ipv4->getUInt(0) == 1)
        {
            return std::ranges::any_of(ips, std::bind_front(std::equal_to<std::string>(), s));
        }
        return false;
    }
};

struct HasAnyIpv4Prefix
{
    static constexpr auto name = "kql_has_any_ipv4_prefix";
    static constexpr auto variadic = true;
    static constexpr auto regex = "([^[:alnum:]]|^)([0-9]{1,3}\\.[0-9]{1,3}\\.[0-9]{1,3}\\.[0-9]{1,3})([^[:alnum:]]|$)";
    static std::string
    transformStringArgument(const ColumnWithTypeAndName & arg, const DataTypePtr & result_type, const ContextPtr & context, size_t row)
    {
        return HasAnyIpv4::transformStringArgument(arg, result_type, context, row);
    }
    static bool checkStringArgument(
        const std::string & arg,
        [[maybe_unused]] const DataTypePtr & result_type,
        [[maybe_unused]] const ContextPtr & context,
        [[maybe_unused]] size_t row)
    {
        const auto n = std::ranges::count(arg, '.');
        return n == 3 || (arg.back() == '.' && n <= 2);
    }
    static bool insertFromArrayElement(
        const Field & value,
        [[maybe_unused]] const DataTypePtr & result_type,
        [[maybe_unused]] const ContextPtr & context,
        std::vector<std::string> & ips)
    {
        const auto value_as_string = toString(value);

        const auto n = std::ranges::count(value_as_string, '.');
        if (n == 3 || (value_as_string.back() == '.' && n <= 2))
        {
            ips.push_back(value_as_string);
            return true;
        }
        return false;
    }
    static bool checkRegexMatch(
        const std::string & s, const DataTypePtr & result_type, const ContextPtr & context, const std::vector<std::string> & ips)
    {
        const ColumnsWithTypeAndName is_ipv4_string_args = {createConstColumnWithTypeAndName<DataTypeString>(s, "ip")};

        const auto is_ipv4 = FunctionFactory::instance()
                                 .get("isIPv4String", context)
                                 ->build(is_ipv4_string_args)
                                 ->execute(is_ipv4_string_args, result_type, 1);

        if (is_ipv4->getUInt(0) == 1)
        {
            return std::ranges::any_of(
                ips,
                [&s](const std::string & str) -> bool { return std::memcmp(str.c_str(), s.c_str(), std::min(str.size(), s.size())) == 0; });
        }
        return false;
    }
};

struct HasIpv4 : HasAnyIpv4
{
    static constexpr auto name = "kql_has_ipv4";
    static constexpr auto variadic = false;
    static constexpr auto regex = "([^[:alnum:]]|^)([0-9]{1,3}\\.[0-9]{1,3}\\.[0-9]{1,3}\\.[0-9]{1,3})([^[:alnum:]]|$)";
};

struct HasIpv4Prefix : HasAnyIpv4Prefix
{
    static constexpr auto name = "kql_has_ipv4_prefix";
    static constexpr auto variadic = false;
    static constexpr auto regex = "([^[:alnum:]]|^)([0-9]{1,3}\\.[0-9]{1,3}\\.[0-9]{1,3}\\.[0-9]{1,3})([^[:alnum:]]|$)";
};

struct HasAnyIpv6
{
    static constexpr auto name = "kql_has_any_ipv6";
    static constexpr auto variadic = true;
    static constexpr auto regex = "([^a-zA-Z0-9:.]|^)([0-9a-fA-F:.]{3,})([^a-zA-Z0-9:.]|$)";
    static std::optional<std::string> ipv6ToHex(const std::string & str, const DataTypePtr & result_type, const ContextPtr & context)
    {
        const ColumnsWithTypeAndName ipv6_string = {createConstColumnWithTypeAndName<DataTypeString>(str, "ipv6")};
        const auto is_ipv6
            = FunctionFactory::instance().get("isIPv6String", context)->build(ipv6_string)->execute(ipv6_string, result_type, 1);
        if (is_ipv6->getUInt(0))
        {
            const auto ipv6_string_to_num = executeFunctionCall(context, "IPv6StringToNum", ipv6_string, 1);
            const ColumnsWithTypeAndName hex_args{asArgument(ipv6_string_to_num, "")};
            const auto [hex_string, _] = executeFunctionCall(context, "hex", hex_args, 1);
            return hex_string->getDataAt(0).toString();
        }
        return std::nullopt;
    }
    static std::string
    transformStringArgument(const ColumnWithTypeAndName & arg, const DataTypePtr & result_type, const ContextPtr & context, size_t row)
    {
        return ipv6ToHex(arg.column->getDataAt(row).toString(), result_type, context).value_or("");
    }
    static bool checkStringArgument(
        const std::string & arg,
        [[maybe_unused]] const DataTypePtr & result_type,
        [[maybe_unused]] const ContextPtr & context,
        [[maybe_unused]] size_t row)
    {
        return !arg.empty();
    }
    static bool
    insertFromArrayElement(const Field & value, const DataTypePtr & result_type, const ContextPtr & context, std::vector<std::string> & ips)
    {
        const auto ipv6_string = ipv6ToHex(toString(value), result_type, context).value_or("");
        if (!ipv6_string.empty())
        {
            ips.push_back(ipv6_string);
            return true;
        }
        return false;
    }
    static bool checkRegexMatch(
        const std::string & s, const DataTypePtr & result_type, const ContextPtr & context, const std::vector<std::string> & ips)
    {
        const auto m = ipv6ToHex(s, result_type, context).value_or("");

        return std::ranges::any_of(ips, std::bind_front(std::equal_to<std::string>(), std::cref(m)));
    }
};

struct HasAnyIpv6Prefix
{
    static constexpr auto name = "kql_has_any_ipv6_prefix";
    static constexpr auto variadic = true;
    static constexpr auto regex = "([^a-zA-Z0-9:.]|^)([0-9a-fA-F:.]{3,})([^a-zA-Z0-9:.]|$)";
    static std::optional<std::string> ipv6ToHex(const std::string & str, const DataTypePtr & result_type, const ContextPtr & context)
    {
        return HasAnyIpv6::ipv6ToHex(str, result_type, context);
    }

    static std::optional<std::vector<uint32_t>> parsePrefix(const std::string & str)
    {
        std::vector<uint32_t> result;

        auto iter = str.cbegin();
        auto iter_end = str.cend();
        const auto ipv6 = boost::spirit::x3::repeat(1, 7)[boost::spirit::x3::hex >> ':'];
        const auto r = boost::spirit::x3::parse(iter, iter_end, ipv6, result);
        if (!r || iter != iter_end)
        {
            return std::nullopt;
        }
        return result;
    }

    static std::optional<std::vector<uint32_t>> parsePrefixEmbeddedIpv4(const std::string & str)
    {
        std::vector<uint32_t> result;
        const auto ipv4_embedded = boost::spirit::x3::repeat(6)[boost::spirit::x3::hex >> ':']
            >> boost::spirit::x3::repeat(1, 3)[boost::spirit::x3::uint_ >> '.'];

        auto iter = str.begin();
        auto iter_end = str.end();

        auto r = boost::spirit::x3::parse(iter, iter_end, ipv4_embedded, result);
        if (!r || iter != iter_end)
        {
            return std::nullopt;
        }
        return result;
    }

    static std::optional<std::string> ipv6PrefixToHex(const std::string & str, const DataTypePtr & result_type, const ContextPtr & context)
    {
        std::vector<uint32_t> vec_v6;
        std::vector<uint32_t> vec_v4;
        if (const auto & last_char = str.back(); last_char == ':')
        {
            const auto result = parsePrefix(str);

            if (!result.has_value())
            {
                return std::nullopt;
            }
            vec_v6 = result.value();
        }
        else if (last_char == '.')
        {
            const auto result = parsePrefixEmbeddedIpv4(str);
            if (!result.has_value())
            {
                return std::nullopt;
            }

            std::copy(result.value().cbegin(), result.value().cbegin() + 6, std::back_inserter(vec_v6));
            std::copy(result.value().cbegin() + 6, result.value().cend(), std::back_inserter(vec_v4));
        }
        else
        {
            return ipv6ToHex(str, result_type, context);
        }

        if (std::ranges::any_of(vec_v6, [](const auto & x) { return x > std::numeric_limits<uint16_t>::max(); })
            || std::ranges::any_of(vec_v4, [](const auto & x) { return x > std::numeric_limits<uint8_t>::max(); }))
        {
            return std::nullopt;
        }
        auto ipv6_hex = std::accumulate(
            vec_v6.cbegin(),
            vec_v6.cend(),
            std::string(),
            [](auto & x, const auto & y) { return std::move(x) + std::format("{:04X}", y); });
        if (!vec_v4.empty())
        {
            ipv6_hex += std::accumulate(
                vec_v4.cbegin(),
                vec_v4.cend(),
                std::string(),
                [](auto & x, const auto & y) { return std::move(x) + std::format("{:02X}", y); });
        }
        return ipv6_hex;
    }
    static std::string
    transformStringArgument(const ColumnWithTypeAndName & arg, const DataTypePtr & result_type, const ContextPtr & context, size_t row)
    {
        return ipv6PrefixToHex(arg.column->getDataAt(row).toString(), result_type, context).value_or("");
    }
    static bool checkStringArgument(const std::string & arg, const DataTypePtr & result_type, const ContextPtr & context, size_t row)
    {
        return HasAnyIpv6::checkStringArgument(arg, result_type, context, row);
    }
    static bool
    insertFromArrayElement(const Field & value, const DataTypePtr & result_type, const ContextPtr & context, std::vector<std::string> & ips)
    {
        const auto ipv6_string = ipv6PrefixToHex(toString(value), result_type, context).value_or("");
        if (!ipv6_string.empty())
        {
            ips.push_back(ipv6_string);
            return true;
        }
        return false;
    }
    static bool checkRegexMatch(
        const std::string & s, const DataTypePtr & result_type, const ContextPtr & context, const std::vector<std::string> & ips)
    {
        const auto m = ipv6ToHex(s, result_type, context).value_or("");

        return std::ranges::any_of(
            ips,
            [&m](const std::string & str) { return std::memcmp(str.c_str(), m.c_str(), std::min(str.size(), m.size())) == 0; });
    }
};

struct HasIpv6 : HasAnyIpv6
{
    static constexpr auto name = "kql_has_ipv6";
    static constexpr auto variadic = false;
    static constexpr auto regex = "([^a-zA-Z0-9:.]|^)([0-9a-fA-F:.]{3,})([^a-zA-Z0-9:.]|$)";
};

struct HasIpv6Prefix : HasAnyIpv6Prefix
{
    static constexpr auto name = "kql_has_ipv6_prefix";
    static constexpr auto variadic = false;
    static constexpr auto regex = "([^a-zA-Z0-9:.]|^)([0-9a-fA-F:.]{3,})([^a-zA-Z0-9:.]|$)";
};

template <typename Func>
class FunctionKqlHasIpGeneric : public IFunction
{
public:
    static constexpr auto name = Func::name;
    static FunctionPtr create(ContextPtr context) { return std::make_shared<FunctionKqlHasIpGeneric>(std::move(context)); }

    explicit FunctionKqlHasIpGeneric(ContextPtr context_) : context(std::move(context_)) { }
    ~FunctionKqlHasIpGeneric() override = default;

    String getName() const override { return name; }
    size_t getNumberOfArguments() const override { return Func::variadic ? 0 : 2; }
    bool isVariadic() const override { return Func::variadic; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        const auto args_length = arguments.size();

        if (args_length < 2)
        {
            throw Exception(
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "Number of arguments for function {} doesn't match: passed {}, should be 2 or more.",
                getName(),
                toString(arguments.size()));
        }

        if (!isStringOrFixedString(arguments.at(0).type))
        {
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Illegal type of argument of function {}", getName());
        }

        if (!isStringOrFixedString(arguments.at(1).type) && !isArray(arguments.at(1).type))
        {
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Illegal type of argument of function {}", getName());
        }

        if (isStringOrFixedString(arguments.at(1).type))
        {
            if (isVariadic())
            {
                const auto are_arguments_valid = std::ranges::all_of(
                    arguments | std::views::drop(2), [](const auto & argument) { return isStringOrFixedString(argument.type); });
                if (!are_arguments_valid)
                    throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Illegal type of argument of function {}", getName());
            }
        }

        else if (!isVariadic() || !isArray(arguments.at(1).type))
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Illegal type of argument of function {}", getName());
        }

        return std::make_shared<DataTypeUInt8>();
    }

    ColumnPtr
    executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, const size_t input_rows_count) const override
    {
        auto result = ColumnUInt8::create();
        auto & result_column = result->getData();

        for (size_t i = 0; i < input_rows_count; ++i)
        {
            bool res = false;
            const auto ips = extractIpsFromArguments(arguments, result_type, context, i);

            std::string source = arguments[0].column->getDataAt(i).toString();
            const std::regex ip_finder(Func::regex);
            std::smatch matches;

            while (!res && std::regex_search(source, matches, ip_finder))
            {
                res = Func::checkRegexMatch(matches[2].str(), result_type, context, ips);

                source = matches.suffix().str();
            }
            result_column.push_back(static_cast<UInt8>(res));
        }
        return result;
    }

private:
    ContextPtr context;

    static std::vector<std::string> extractIpsFromArguments(
        const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, const ContextPtr & context, size_t row)
    {
        std::vector<std::string> ips;
        const auto is_ipv4_string = [&, result_type](const DB::ColumnsWithTypeAndName & args)
        { return DB::FunctionFactory::instance().get("isIPv4String", context)->build(args)->execute(args, result_type, 1); };

        if (isStringOrFixedString(arguments.at(1).type))
        {
            std::ranges::copy_if(
                arguments | std::views::drop(1)
                    | std::views::transform([&row, &result_type, &context](const ColumnWithTypeAndName & arg)
                                            { return Func::transformStringArgument(arg, result_type, context, row); }),
                std::back_inserter(ips),
                [&row, &result_type, &context](const std::string & arg)
                { return Func::checkStringArgument(arg, result_type, context, row); });
        }

        else if (isArray(arguments.at(1).type))
        {
            Field array0;
            arguments[1].column->get(row, array0);
            const auto len0 = array0.get<Array>().size();

            for (size_t j = 0; j < len0; ++j)
            {
                if (const auto & value = array0.get<Array>().at(j); value.getType() == Field::Types::String)
                {
                    Func::insertFromArrayElement(value, result_type, context, ips);
                }
            }
        }
        return ips;
    }
};

REGISTER_FUNCTION(FunctionKqlHasIpGeneric)
{
    factory.registerFunction<FunctionKqlHasIpGeneric<HasAnyIpv4>>();
    factory.registerFunction<FunctionKqlHasIpGeneric<HasIpv4>>();
    factory.registerFunction<FunctionKqlHasIpGeneric<HasAnyIpv4Prefix>>();
    factory.registerFunction<FunctionKqlHasIpGeneric<HasIpv4Prefix>>();

    factory.registerFunction<FunctionKqlHasIpGeneric<HasAnyIpv6>>();
    factory.registerFunction<FunctionKqlHasIpGeneric<HasIpv6>>();
    factory.registerFunction<FunctionKqlHasIpGeneric<HasAnyIpv6Prefix>>();
    factory.registerFunction<FunctionKqlHasIpGeneric<HasIpv6Prefix>>();
}
}
