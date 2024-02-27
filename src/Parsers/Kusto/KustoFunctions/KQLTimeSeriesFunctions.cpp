#include "KQLTimeSeriesFunctions.h"
#include "KQLFunctionFactory.h"
#include <Parsers/CommonParsers.h>
#include <format>

namespace DB::ErrorCodes
{
extern const int NOT_IMPLEMENTED;
}
namespace DB
{

bool SeriesFir::convertImpl(String & out, IParser::Pos & pos)
{
    String res = String(pos->begin, pos->end);
    out = res;
    return false;
}

bool SeriesIir::convertImpl(String & out, IParser::Pos & pos)
{
    String res = String(pos->begin, pos->end);
    out = res;
    return false;
}

bool SeriesFitLine::convertImpl(String & out, IParser::Pos & pos)
{
    String res = String(pos->begin, pos->end);
    out = res;
    return false;
}

bool SeriesFitLineDynamic::convertImpl(String & out, IParser::Pos & pos)
{
    String res = String(pos->begin, pos->end);
    out = res;
    return false;
}

bool SeriesFit2lines::convertImpl(String & out, IParser::Pos & pos)
{
    String res = String(pos->begin, pos->end);
    out = res;
    return false;
}

bool SeriesFit2linesDynamic::convertImpl(String & out, IParser::Pos & pos)
{
    String res = String(pos->begin, pos->end);
    out = res;
    return false;
}

bool SeriesOutliers::convertImpl(String & out, IParser::Pos & pos)
{
    const auto fn_name = getKQLFunctionName(pos);
    if (fn_name.empty())
        return false;

    const auto series = getArgument(fn_name, pos);
    const auto kind = getOptionalArgument(fn_name, pos);
    const auto ignore_value = getOptionalArgument(fn_name, pos); // This value is not used currently
    const auto min_percentile = getOptionalArgument(fn_name, pos);
    const auto max_percentile = getOptionalArgument(fn_name, pos);

    if(kind && *kind == "tukey"){
        out = std::format("seriesOutliersDetectTukey({0})", series);
    }
    else {
        out = std::format("seriesOutliersDetectTukey({0},{1},{2},1.50)", 
        series,
        min_percentile.value_or("10"),
        max_percentile.value_or("90"));
    }

    return true;
}

bool SeriesPeriodsDetect::convertImpl(String & out, IParser::Pos & pos)
{
    const auto fn_name = getKQLFunctionName(pos);
    if (fn_name.empty())
        return false;

    const auto series = getArgument(fn_name, pos);
    out = std::format("seriesPeriodDetectFFT({0})", series);

    return true;
}

bool SeriesPeriodsValidate::convertImpl(String & out, IParser::Pos & pos)
{
    String res = String(pos->begin, pos->end);
    out = res;
    return false;
}

bool SeriesStatsDynamic::convertImpl(String & out, IParser::Pos & pos)
{
    String res = String(pos->begin, pos->end);
    out = res;
    return false;
}

bool SeriesStats::convertImpl(String & out, IParser::Pos & pos)
{
    String res = String(pos->begin, pos->end);
    out = res;
    return false;
}

bool SeriesFillBackward::convertImpl(String & out, IParser::Pos & pos)
{
    String res = String(pos->begin, pos->end);
    out = res;
    return false;
}

bool SeriesFillConst::convertImpl(String & out, IParser::Pos & pos)
{
    String res = String(pos->begin, pos->end);
    out = res;
    return false;
}

bool SeriesFillForward::convertImpl(String & out, IParser::Pos & pos)
{
    String res = String(pos->begin, pos->end);
    out = res;
    return false;
}

bool SeriesFillLinear::convertImpl(String & out, IParser::Pos & pos)
{
    String res = String(pos->begin, pos->end);
    out = res;
    return false;
}

bool SeriesDecomposeAnomalies::convertImpl(String & out, IParser::Pos & pos)
{
    const auto fn_name = getKQLFunctionName(pos);
    if (fn_name.empty())
        return false;

    const auto series = getArgument(fn_name, pos);
    const auto threshold = getOptionalArgument(fn_name, pos);
    const auto seasonality = getOptionalArgument(fn_name, pos);
    const auto trend = getOptionalArgument(fn_name, pos);
    const auto test_points = getOptionalArgument(fn_name, pos);
    const auto ad_method = getOptionalArgument(fn_name, pos);
    const auto seasonality_threshold = getOptionalArgument(fn_name, pos); //This value is not used currently

    if(test_points && *test_points != "0")
        throw DB::Exception(DB::ErrorCodes::NOT_IMPLEMENTED, "only 0 test_points are supported");

    if(trend && *trend != "'linefit'")
        throw DB::Exception(DB::ErrorCodes::NOT_IMPLEMENTED, "only linefit value for trend is supported");

    out = std::format(
        "seriesAnomaliesDetectDecompose({0},{1},{2},{3})",
        series,
        threshold.value_or("1.5"),
        seasonality.value_or("-1"),
        ad_method.value_or("'ctukey'"));

    return true;
}

bool SeriesDecompose::convertImpl(String & out, IParser::Pos & pos)
{
    const auto fn_name = getKQLFunctionName(pos);
    if (fn_name.empty())
        return false;

    const auto series = getArgument(fn_name, pos);
    const auto seasonality = getOptionalArgument(fn_name, pos);
    const auto trend = getOptionalArgument(fn_name, pos);
    const auto test_points = getOptionalArgument(fn_name, pos);
    const auto seasonality_threshold = getOptionalArgument(fn_name, pos); //This value is not used currently

    if(test_points && *test_points != "0")
        throw DB::Exception(DB::ErrorCodes::NOT_IMPLEMENTED, "only 0 test_points are supported");

    if(trend && *trend != "'linefit'")
        throw DB::Exception(DB::ErrorCodes::NOT_IMPLEMENTED, "only linefit value for trend is supported");

    if(!seasonality || (seasonality && *seasonality == "- 1"))
    {
        out = std::format("seriesDecomposeSTL({0}, seriesPeriodDetectFFT({0}))",series);
    }
    else 
    {
        out = std::format("seriesDecomposeSTL({0},{1})",series, seasonality.value_or("0"));
    }

    return true;
}

}
