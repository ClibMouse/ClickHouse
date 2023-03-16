#pragma once

#include <format>
#include <ranges>
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

namespace DB
{
namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int BAD_ARGUMENTS;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}


enum class ArgumentPolicy
{
    Single,
    Variadic
};

enum class SearchType
{
    IPv6,
    IPv6_Prefix,
    IPv4,
    IPv4_Prefix
};

static std::string ipv6ToHex(const std::string & str, const DB::DataTypePtr & result_type, const DB::ContextPtr & context)
{
    const DB::ColumnsWithTypeAndName ipv6_string = {DB::createConstColumnWithTypeAndName<DB::DataTypeString>(str, "ipv6")};
    const auto is_ipv6
        = DB::FunctionFactory::instance().get("isIPv6String", context)->build(ipv6_string)->execute(ipv6_string, result_type, 1);
    if (is_ipv6->getUInt(0))
    {
        const auto ipv6_string_to_num = DB::executeFunctionCall(context, "IPv6StringToNum", ipv6_string, 1);
        const DB::ColumnsWithTypeAndName hex_args{DB::asArgument(ipv6_string_to_num, "")};
        const auto [hex_string, _] = DB::executeFunctionCall(context, "hex", hex_args, 1);
        return hex_string->getDataAt(0).toString();
    }
    return "";
}

static std::string ipv6PrefixToHex(const std::string & str, const DB::DataTypePtr & result_type, const DB::ContextPtr & context)
{
    std::vector<uint32_t> vec_v6;
    std::vector<uint32_t> vec_v4;
    const auto last_char = str.back();

    if (last_char == ':')
    {
        const auto ipv6 = boost::spirit::x3::hex % ':';
        const auto r = boost::spirit::x3::parse(str.begin(), str.end(), ipv6, vec_v6);
        if (!r || vec_v6.empty() || vec_v6.size() > 7)
        {
            return "";
        }
    }
    else if (last_char == '.')
    {
        const auto ipv6_prefix = boost::spirit::x3::repeat(6)[boost::spirit::x3::hex >> ':'];
        const auto ipv4_embedded = boost::spirit::x3::uint_ % '.';

        auto iter = str.begin();
        auto iter_end = str.end();

        auto r = boost::spirit::x3::parse(iter, iter_end, ipv6_prefix, vec_v6);
        if (!r || vec_v6.empty() || vec_v6.size() != 6)
        {
            return "";
        }

        r = boost::spirit::x3::parse(iter, iter_end, ipv4_embedded, vec_v4);

        if (!r || vec_v4.empty() || vec_v4.size() >= 4)
        {
            return "";
        }
    }
    else
    {
        return ipv6ToHex(str, result_type, context);
    }

    if (std::ranges::any_of(vec_v6, [](const auto & x) { return x > std::numeric_limits<uint16_t>::max(); })
        || std::ranges::any_of(vec_v4, [](const auto & x) { return x > std::numeric_limits<uint8_t>::max(); }))
    {
        return "";
    }
    auto ipv6_hex = std::accumulate(
        std::next(vec_v6.begin()),
        vec_v6.end(),
        std::format("{:04X}", vec_v6.front()),
        [](const auto & x, const auto & y) { return x + std::format("{:04X}", y); });
    if (!vec_v4.empty())
    {
        ipv6_hex += std::accumulate(
            std::next(vec_v4.begin()),
            vec_v4.end(),
            std::format("{:02X}", vec_v4.front()),
            [](const std::string & x, const auto & y) { return x + std::format("{:02X}", y); });
    }
    return ipv6_hex;
}

static std::vector<std::string> extractIpsFromArguments(
    const DB::ColumnsWithTypeAndName & arguments,
    const DB::DataTypePtr & result_type,
    const DB::ContextPtr & context,
    size_t row,
    const SearchType & search_type)
{
    std::vector<std::string> ips;
    const auto is_ipv4_string = [&, result_type](const DB::ColumnsWithTypeAndName & args)
    { return DB::FunctionFactory::instance().get("isIPv4String", context)->build(args)->execute(args, result_type, 1); };

    if (DB::isStringOrFixedString(arguments.at(1).type))
    {
        std::ranges::copy_if(
            arguments | std::views::drop(1)
                | std::views::transform(
                    [&row, &result_type, &context, &search_type](const DB::ColumnWithTypeAndName & arg)
                    {
                        if (search_type == SearchType::IPv6_Prefix)
                        {
                            return ipv6PrefixToHex(arg.column->getDataAt(row).toString(), result_type, context);
                        }
                        else if (search_type == SearchType::IPv6)
                        {
                            return ipv6ToHex(arg.column->getDataAt(row).toString(), result_type, context);
                        }
                        else
                        {
                            return arg.column->getDataAt(row).toString();
                        }
                    }),
            std::back_inserter(ips),
            [&is_ipv4_string, &search_type](const std::string & arg)
            {
                if (search_type == SearchType::IPv4)
                {
                    const DB::ColumnsWithTypeAndName is_ipv4_string_args
                        = {DB::createConstColumnWithTypeAndName<DB::DataTypeString>(arg, "ip")};
                    const auto is_ipv4 = is_ipv4_string(is_ipv4_string_args);
                    return is_ipv4->getUInt(0) == 1;
                }
                else if (search_type == SearchType::IPv4_Prefix)
                {
                    const auto n = std::ranges::count(arg, '.');
                    return n == 3 || (arg.back() == '.' && n <= 2);
                }
                return !arg.empty();
            });
    }

    else if (isArray(arguments.at(1).type))
    {
        DB::Field array0;
        arguments[1].column->get(row, array0);
        const auto len0 = array0.get<DB::Array>().size();

        for (size_t j = 0; j < len0; ++j)
        {
            if (const auto & value = array0.get<DB::Array>().at(j); value.getType() == DB::Field::Types::String)
            {
                if (search_type == SearchType::IPv4)
                {
                    const auto value_as_string = toString(value);
                    const DB::ColumnsWithTypeAndName is_ipv4_string_args
                        = {DB::createConstColumnWithTypeAndName<DB::DataTypeString>(value_as_string, "ip")};
                    const auto is_ipv4 = is_ipv4_string(is_ipv4_string_args);
                    if (is_ipv4->getUInt(0) == 1)
                    {
                        ips.push_back(value_as_string);
                    }
                }

                else if (search_type == SearchType::IPv4_Prefix)
                {
                    const auto value_as_string = toString(value);

                    const auto n = std::ranges::count(value_as_string, '.');
                    if (n == 3 || (value_as_string.back() == '.' && n <= 2))
                    {
                        ips.push_back(value_as_string);
                    }
                }
                else
                {
                    const auto ipv6_string = search_type == SearchType::IPv6_Prefix ? ipv6PrefixToHex(toString(value), result_type, context)
                                                                                    : ipv6ToHex(toString(value), result_type, context);
                    if (!ipv6_string.empty())
                    {
                        ips.push_back(ipv6_string);
                    }
                }
            }
        }
    }
    return ips;
}

template <ArgumentPolicy ap>
DataTypePtr getReturnType(const ColumnsWithTypeAndName & arguments, const std::string & name)
{
    const auto args_length = arguments.size();

    if (args_length < 2)
    {
        throw Exception(
            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
            "Number of arguments for function {} doesn't match: passed {}, should be 2 or more.",
            name,
            toString(arguments.size()));
    }

    if (!isStringOrFixedString(arguments.at(0).type))
    {
        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Illegal type of argument of function {}", name);
    }

    if (!isStringOrFixedString(arguments.at(1).type) && !isArray(arguments.at(1).type))
    {
        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Illegal type of argument of function {}", name);
    }

    if (isStringOrFixedString(arguments.at(1).type))
    {
        if constexpr (ap == ArgumentPolicy::Variadic)
        {
            const auto are_arguments_valid = std::ranges::all_of(
                arguments | std::views::drop(2), [](const auto & argument) { return isStringOrFixedString(argument.type); });
            if (!are_arguments_valid)
                throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Illegal type of argument of function {}", name);
        }
    }

    else if (ap == ArgumentPolicy::Single || !isArray(arguments.at(1).type))
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Illegal type of argument of function {}", name);
    }

    return std::make_shared<DataTypeUInt8>();
}
}
