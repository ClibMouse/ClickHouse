#include <Parsers/IParserBase.h>
#include <Parsers/ParserSetQuery.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/Kusto/ParserKQLQuery.h>
#include <Parsers/Kusto/ParserKQLStatement.h>
#include <Parsers/Kusto/KustoFunctions/IParserKQLFunction.h>
#include <Parsers/Kusto/KustoFunctions/KQLDateTimeFunctions.h>
#include <Parsers/Kusto/KustoFunctions/KQLStringFunctions.h>
#include <Parsers/Kusto/KustoFunctions/KQLDynamicFunctions.h>
#include <Parsers/Kusto/KustoFunctions/KQLCastingFunctions.h>
#include <Parsers/Kusto/KustoFunctions/KQLAggregationFunctions.h>
#include <Parsers/Kusto/KustoFunctions/KQLTimeSeriesFunctions.h>
#include <Parsers/Kusto/KustoFunctions/KQLIPFunctions.h>
#include <Parsers/Kusto/KustoFunctions/KQLBinaryFunctions.h>
#include <Parsers/Kusto/KustoFunctions/KQLGeneralFunctions.h>
#include <Parsers/Kusto/ParserKQLDateTypeTimespan.h>
#include <format>
#include <boost/lexical_cast.hpp>


namespace DB
{
namespace DB::ErrorCodes
{
extern const int BAD_ARGUMENTS;
}

bool Bin::convertImpl(String & out,IParser::Pos & pos)
{
    const String fn_name = getKQLFunctionName(pos);
    if (fn_name.empty())
        return false;
    ++pos;
    String origal_expr(pos->begin, pos->end);
     
    --pos;
    String value = getArgument(fn_name, pos);
    ++pos;
    String origal_round_to(pos->begin, pos->end);
    --pos;
    String round_to = getArgument(fn_name, pos);
    //remove spaces between minus and number
    round_to.erase(std::remove_if(round_to.begin(), round_to.end(), isspace), round_to.end());

    auto t = std::format("toFloat64({})", value);
    auto bin_size =  std::format("toFloat64({})", round_to);
    int scale = 0;
    String decimal_val;

    ParserKQLDateTypeTimespan::KQLTimespanUint time_unit  = ParserKQLDateTypeTimespan().getTimespanUnit(origal_round_to);
    switch(time_unit)
    {
        case ParserKQLDateTypeTimespan::KQLTimespanUint::millisec:
            scale = 3;
            break;
        case ParserKQLDateTypeTimespan::KQLTimespanUint::microsec:
            scale = 6;     
            break;
        case ParserKQLDateTypeTimespan::KQLTimespanUint::nanosec:
            scale = 9;
            break;
        case ParserKQLDateTypeTimespan::KQLTimespanUint::tick:
            scale = 7;
            break;
        default:
            scale = 4;
    }
    if (scale == 9 && ParserKQLDateTypeTimespan().getTimespan(origal_round_to) < 100)
    {
        out = "NULL";
        return true;
    }
    if (origal_expr == "datetime" || origal_expr == "date")
        out = std::format("toDateTime64(toInt64({0} / {1} ) * {1}, {2}, 'UTC')", t, bin_size, scale); 

    else if (origal_expr == "timespan" || origal_expr =="time" || ParserKQLDateTypeTimespan().parseConstKQLTimespan(origal_expr))
    {
        String bin_value = std::format("toInt64({0} / {1} ) * {1}", t, bin_size);
        decimal_val = std::format("countSubstrings(({0})::String, '.') = 0 ? '': substr(({0})::String, position(({0})::String,'.') + 1)", bin_value);
        out = std::format("concat(toString( toInt32(({0}) / 86400) as x),'.' ,toString(toInt32(x % 86400 / 3600)),':', toString( toInt32(x % 86400 % 3600 / 60)),':',toString( toInt32( x % 86400 % 3600 % 60 / 60)), empty({1}) ? '' : concat('.' , substr({1} , 1, {2})) )", bin_value, decimal_val, scale);
    }
    else 
        out = std::format(" CAST(multiIf({1} > 0, toInt64({0} / {1} ) * {1} , {1} < 0 AND abs({1}) < {0} , ceil({0}/abs({1})) * abs({1}) , abs({1}) > {0} AND {1} < 0  , {1} , NULL  ) , toTypeName({2}))", t, bin_size, value);
    
    return true;
}

bool BinAt::convertImpl(String & out,IParser::Pos & pos)
{
    double bin_size;
    const String fn_name = getKQLFunctionName(pos);
    if (fn_name.empty())
        return false;

    ++pos;
    String origal_expr(pos->begin, pos->end);
    String expression_str = getConvertedArgument(fn_name, pos);

    ++pos;
    String bin_size_str = getConvertedArgument(fn_name, pos);

    ++pos;
    String fixed_point_str = getConvertedArgument(fn_name, pos);

    auto t1 = std::format("toFloat64({})", fixed_point_str);
    auto t2 = std::format("toFloat64({})", expression_str);
    int dir = t2 >= t1 ? 0 : -1;
    bin_size =  std::stod(bin_size_str);

    if (origal_expr == "datetime" || origal_expr == "date")
    {
        out = std::format("toDateTime64({} + toInt64(({} - {}) / {} + {}) * {}, 9, 'UTC')", t1, t2, t1, bin_size, dir, bin_size);
    }
    else if (origal_expr == "timespan" || origal_expr =="time" || ParserKQLDateTypeTimespan().parseConstKQLTimespan(origal_expr))
    {
        String bin_value = std::format("{} + toInt64(({} - {}) / {} + {}) * {}", t1, t2, t1, bin_size, dir, bin_size);
        out = std::format("concat(toString( toInt32((({}) as x) / 3600)),':', toString( toInt32(x % 3600 / 60)),':',toString( toInt32(x % 3600 % 60)))", bin_value);
    }
    else
    {
        out = std::format("{} + toInt64(({} - {}) / {} + {}) * {}", t1, t2, t1, bin_size, dir, bin_size);
    }
    return true;
}

bool Case::convertImpl(String & out,IParser::Pos & pos)
{
    return directMapping(out, pos, "multiIf");
}

}
