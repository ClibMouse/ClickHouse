#include "KQLFunctionFactory.h"
#include "KQLAggregationFunctions.h"
#include "KQLBinaryFunctions.h"
#include "KQLCastingFunctions.h"
#include "KQLDataTypeFunctions.h"
#include "KQLDateTimeFunctions.h"
#include "KQLDynamicFunctions.h"
#include "KQLGeneralFunctions.h"
#include "KQLIPFunctions.h"
#include "KQLMathematicalFunctions.h"
#include "KQLStringFunctions.h"
#include "KQLTimeSeriesFunctions.h"

#include <unordered_map>

namespace
{
enum class KQLFunction : uint16_t
{
    none,
    ago,
    datetime_add,
    datetime_part,
    datetime_diff,
    dayofmonth,
    dayofweek,
    dayofyear,
    endofday,
    endofweek,
    endofyear,
    endofmonth,
    monthofyear,
    format_datetime,
    format_timespan,
    getmonth,
    getyear,
    hourofday,
    make_timespan,
    make_datetime,
    now,
    startofday,
    startofmonth,
    startofweek,
    startofyear,
    todatetime,
    totimespan,
    unixtime_microseconds_todatetime,
    unixtime_milliseconds_todatetime,
    unixtime_nanoseconds_todatetime,
    unixtime_seconds_todatetime,
    week_of_year,

    base64_encode_tostring,
    base64_encode_fromguid,
    base64_decode_tostring,
    base64_decode_toarray,
    base64_decode_toguid,
    countof,
    extract,
    extract_all,
    extract_json,
    has_any_index,
    indexof,
    isempty,
    isnan,
    isnotempty,
    isnotnull,
    isnull,
    parse_command_line,
    parse_csv,
    parse_json,
    parse_url,
    parse_urlquery,
    parse_version,
    replace_regex,
    reverse,
    split,
    strcat,
    strcat_delim,
    strcmp,
    strlen,
    strrep,
    substring,
    tolower,
    toupper,
    translate,
    trim,
    trim_end,
    trim_start,
    url_decode,
    url_encode,

    array_concat,
    array_iif,
    array_index_of,
    array_length,
    array_reverse,
    array_rotate_left,
    array_rotate_right,
    array_shift_left,
    array_shift_right,
    array_slice,
    array_sort_asc,
    array_sort_desc,
    array_split,
    array_sum,
    bag_keys,
    bag_merge,
    bag_remove_keys,
    jaccard_index,
    pack,
    pack_all,
    pack_array,
    repeat,
    set_difference,
    set_has_element,
    set_intersect,
    set_union,
    treepath,
    zip,

    tobool,
    todouble,
    toint,
    tolong,
    tostring,
    todecimal,

    arg_max,
    arg_min,
    avg,
    avgif,
    binary_all_and,
    binary_all_or,
    binary_all_xor,
    buildschema,
    count,
    countif,
    dcount,
    dcountif,
    make_bag,
    make_bag_if,
    make_list,
    make_list_if,
    make_list_with_nulls,
    make_set,
    make_set_if,
    max,
    maxif,
    min,
    minif,
    percentile,
    percentilew,
    percentiles,
    percentiles_array,
    percentilesw,
    percentilesw_array,
    stdev,
    stdevif,
    sum,
    sumif,
    take_any,
    take_anyif,
    variance,
    varianceif,
    count_distinct,
    count_distinctif,

    series_fir,
    series_iir,
    series_fit_line,
    series_fit_line_dynamic,
    series_fit_2lines,
    series_fit_2lines_dynamic,
    series_outliers,
    series_periods_detect,
    series_periods_validate,
    series_stats_dynamic,
    series_stats,
    series_fill_backward,
    series_fill_const,
    series_fill_forward,
    series_fill_linear,

    ipv4_compare,
    ipv4_is_in_range,
    ipv4_is_match,
    ipv4_is_private,
    ipv4_netmask_suffix,
    parse_ipv4,
    parse_ipv4_mask,
    has_ipv6,
    has_any_ipv6,
    has_ipv6_prefix,
    has_any_ipv6_prefix,
    ipv6_compare,
    ipv6_is_match,
    parse_ipv6,
    parse_ipv6_mask,
    format_ipv4,
    format_ipv4_mask,

    binary_and,
    binary_not,
    binary_or,
    binary_shift_left,
    binary_shift_right,
    binary_xor,
    bitset_count_ones,

    bin,
    bin_at,
    kase,
    iff,
    iif,

    datatype_bool,
    datatype_datetime,
    datatype_dynamic,
    datatype_guid,
    datatype_int,
    datatype_long,
    datatype_real,
    datatype_timespan,
    datatype_decimal
};

const std::unordered_map<String, KQLFunction> KQL_FUNCTIONS{
    {"ago", KQLFunction::ago},
    {"datetime_add", KQLFunction::datetime_add},
    {"datetime_part", KQLFunction::datetime_part},
    {"datetime_diff", KQLFunction::datetime_diff},
    {"dayofmonth", KQLFunction::dayofmonth},
    {"dayofweek", KQLFunction::dayofweek},
    {"dayofyear", KQLFunction::dayofyear},
    {"endofday", KQLFunction::endofday},
    {"endofweek", KQLFunction::endofweek},
    {"endofyear", KQLFunction::endofyear},
    {"endofmonth", KQLFunction::endofmonth},

    {"format_datetime", KQLFunction::format_datetime},
    {"format_timespan", KQLFunction::format_timespan},
    {"getmonth", KQLFunction::getmonth},
    {"getyear", KQLFunction::getyear},
    {"hourofday", KQLFunction::hourofday},
    {"make_timespan", KQLFunction::make_timespan},
    {"make_datetime", KQLFunction::make_datetime},
    {"now", KQLFunction::now},
    {"startofday", KQLFunction::startofday},
    {"startofmonth", KQLFunction::startofmonth},
    {"startofweek", KQLFunction::startofweek},
    {"startofyear", KQLFunction::startofyear},
    {"todatetime", KQLFunction::todatetime},
    {"totimespan", KQLFunction::totimespan},
    {"unixtime_microseconds_todatetime", KQLFunction::unixtime_microseconds_todatetime},
    {"unixtime_milliseconds_todatetime", KQLFunction::unixtime_milliseconds_todatetime},
    {"unixtime_nanoseconds_todatetime", KQLFunction::unixtime_nanoseconds_todatetime},
    {"unixtime_seconds_todatetime", KQLFunction::unixtime_seconds_todatetime},
    {"week_of_year", KQLFunction::week_of_year},
    {"monthofyear", KQLFunction::monthofyear},
    {"base64_encode_tostring", KQLFunction::base64_encode_tostring},
    {"base64_encode_fromguid", KQLFunction::base64_encode_fromguid},
    {"base64_decode_tostring", KQLFunction::base64_decode_tostring},
    {"base64_decode_toarray", KQLFunction::base64_decode_toarray},
    {"base64_decode_toguid", KQLFunction::base64_decode_toguid},
    {"countof", KQLFunction::countof},
    {"extract", KQLFunction::extract},
    {"extract_all", KQLFunction::extract_all},
    {"extract_json", KQLFunction::extract_json},
    {"extractjson", KQLFunction::extract_json},
    {"has_any_index", KQLFunction::has_any_index},
    {"indexof", KQLFunction::indexof},
    {"isempty", KQLFunction::isempty},
    {"isnan", KQLFunction::isnan},
    {"isnotempty", KQLFunction::isnotempty},
    {"notempty", KQLFunction::isnotempty},
    {"isnotnull", KQLFunction::isnotnull},
    {"notnull", KQLFunction::isnotnull},
    {"isnull", KQLFunction::isnull},
    {"parse_command_line", KQLFunction::parse_command_line},
    {"parse_csv", KQLFunction::parse_csv},
    {"parse_json", KQLFunction::parse_json},
    {"parse_url", KQLFunction::parse_url},
    {"parse_urlquery", KQLFunction::parse_urlquery},
    {"parse_version", KQLFunction::parse_version},
    {"replace_regex", KQLFunction::replace_regex},
    {"reverse", KQLFunction::reverse},
    {"split", KQLFunction::split},
    {"strcat", KQLFunction::strcat},
    {"strcat_delim", KQLFunction::strcat_delim},
    {"strcmp", KQLFunction::strcmp},
    {"strlen", KQLFunction::strlen},
    {"strrep", KQLFunction::strrep},
    {"substring", KQLFunction::substring},
    {"tolower", KQLFunction::tolower},
    {"toupper", KQLFunction::toupper},
    {"translate", KQLFunction::translate},
    {"trim", KQLFunction::trim},
    {"trim_end", KQLFunction::trim_end},
    {"trim_start", KQLFunction::trim_start},
    {"url_decode", KQLFunction::url_decode},
    {"url_encode", KQLFunction::url_encode},

    {"array_concat", KQLFunction::array_concat},
    {"array_iff", KQLFunction::array_iif},
    {"array_iif", KQLFunction::array_iif},
    {"array_index_of", KQLFunction::array_index_of},
    {"array_length", KQLFunction::array_length},
    {"array_reverse", KQLFunction::array_reverse},
    {"array_rotate_left", KQLFunction::array_rotate_left},
    {"array_rotate_right", KQLFunction::array_rotate_right},
    {"array_shift_left", KQLFunction::array_shift_left},
    {"array_shift_right", KQLFunction::array_shift_right},
    {"array_slice", KQLFunction::array_slice},
    {"array_sort_asc", KQLFunction::array_sort_asc},
    {"array_sort_desc", KQLFunction::array_sort_desc},
    {"array_split", KQLFunction::array_split},
    {"array_sum", KQLFunction::array_sum},
    {"bag_keys", KQLFunction::bag_keys},
    {"bag_merge", KQLFunction::bag_merge},
    {"bag_remove_keys", KQLFunction::bag_remove_keys},
    {"jaccard_index", KQLFunction::jaccard_index},
    {"pack", KQLFunction::pack},
    {"pack_all", KQLFunction::pack_all},
    {"pack_array", KQLFunction::pack_array},
    {"repeat", KQLFunction::repeat},
    {"set_difference", KQLFunction::set_difference},
    {"set_has_element", KQLFunction::set_has_element},
    {"set_intersect", KQLFunction::set_intersect},
    {"set_union", KQLFunction::set_union},
    {"treepath", KQLFunction::treepath},
    {"zip", KQLFunction::zip},

    {"tobool", KQLFunction::tobool},
    {"toboolean", KQLFunction::tobool},
    {"todouble", KQLFunction::todouble},
    {"toint", KQLFunction::toint},
    {"tolong", KQLFunction::tolong},
    {"toreal", KQLFunction::todouble},
    {"tostring", KQLFunction::tostring},
    {"totimespan", KQLFunction::totimespan},
    {"todecimal", KQLFunction::todecimal},

    {"arg_max", KQLFunction::arg_max},
    {"arg_min", KQLFunction::arg_min},
    {"avg", KQLFunction::avg},
    {"avgif", KQLFunction::avgif},
    {"binary_all_and", KQLFunction::binary_all_and},
    {"binary_all_or", KQLFunction::binary_all_or},
    {"binary_all_xor", KQLFunction::binary_all_xor},
    {"buildschema", KQLFunction::buildschema},
    {"count", KQLFunction::count},
    {"countif", KQLFunction::countif},
    {"dcount", KQLFunction::dcount},
    {"dcountif", KQLFunction::dcountif},
    {"make_bag", KQLFunction::make_bag},
    {"make_bag_if", KQLFunction::make_bag_if},
    {"make_list", KQLFunction::make_list},
    {"make_list_if", KQLFunction::make_list_if},
    {"make_list_with_nulls", KQLFunction::make_list_with_nulls},
    {"make_set", KQLFunction::make_set},
    {"make_set_if", KQLFunction::make_set_if},
    {"max", KQLFunction::max},
    {"maxif", KQLFunction::maxif},
    {"min", KQLFunction::min},
    {"minif", KQLFunction::minif},
    {"percentile", KQLFunction::percentile},
    {"percentilew", KQLFunction::percentilew},
    {"percentiles", KQLFunction::percentiles},
    {"percentiles_array", KQLFunction::percentiles_array},
    {"percentilesw", KQLFunction::percentilesw},
    {"percentilesw_array", KQLFunction::percentilesw_array},
    {"stdev", KQLFunction::stdev},
    {"stdevif", KQLFunction::stdevif},
    {"sum", KQLFunction::sum},
    {"sumif", KQLFunction::sumif},
    {"take_any", KQLFunction::take_any},
    {"take_anyif", KQLFunction::take_anyif},
    {"variance", KQLFunction::variance},
    {"varianceif", KQLFunction::varianceif},
    {"count_distinct", KQLFunction::count_distinct},
    {"count_distinctif", KQLFunction::count_distinctif},

    {"series_fir", KQLFunction::series_fir},
    {"series_iir", KQLFunction::series_iir},
    {"series_fit_line", KQLFunction::series_fit_line},
    {"series_fit_line_dynamic", KQLFunction::series_fit_line_dynamic},
    {"series_fit_2lines", KQLFunction::series_fit_2lines},
    {"series_fit_2lines_dynamic", KQLFunction::series_fit_2lines_dynamic},
    {"series_outliers", KQLFunction::series_outliers},
    {"series_periods_detect", KQLFunction::series_periods_detect},
    {"series_periods_validate", KQLFunction::series_periods_validate},
    {"series_stats_dynamic", KQLFunction::series_stats_dynamic},
    {"series_stats", KQLFunction::series_stats},
    {"series_fill_backward", KQLFunction::series_fill_backward},
    {"series_fill_const", KQLFunction::series_fill_const},
    {"series_fill_forward", KQLFunction::series_fill_forward},
    {"series_fill_linear", KQLFunction::series_fill_linear},

    {"ipv4_compare", KQLFunction::ipv4_compare},
    {"ipv4_is_in_range", KQLFunction::ipv4_is_in_range},
    {"ipv4_is_match", KQLFunction::ipv4_is_match},
    {"ipv4_is_private", KQLFunction::ipv4_is_private},
    {"ipv4_netmask_suffix", KQLFunction::ipv4_netmask_suffix},
    {"parse_ipv4", KQLFunction::parse_ipv4},
    {"parse_ipv4_mask", KQLFunction::parse_ipv4_mask},
    {"ipv6_compare", KQLFunction::ipv6_compare},
    {"ipv6_is_match", KQLFunction::ipv6_is_match},
    {"parse_ipv6", KQLFunction::parse_ipv6},
    {"parse_ipv6_mask", KQLFunction::parse_ipv6_mask},
    {"format_ipv4", KQLFunction::format_ipv4},
    {"format_ipv4_mask", KQLFunction::format_ipv4_mask},

    {"binary_and", KQLFunction::binary_and},
    {"binary_not", KQLFunction::binary_not},
    {"binary_or", KQLFunction::binary_or},
    {"binary_shift_left", KQLFunction::binary_shift_left},
    {"binary_shift_right", KQLFunction::binary_shift_right},
    {"binary_xor", KQLFunction::binary_xor},
    {"bitset_count_ones", KQLFunction::bitset_count_ones},

    {"bin", KQLFunction::bin},
    {"bin_at", KQLFunction::bin_at},
    {"case", KQLFunction::kase},
    {"iff", KQLFunction::iff},
    {"iif", KQLFunction::iif},

    {"bool", KQLFunction::datatype_bool},
    {"boolean", KQLFunction::datatype_bool},
    {"datetime", KQLFunction::datatype_datetime},
    {"date", KQLFunction::datatype_datetime},
    {"dynamic", KQLFunction::datatype_dynamic},
    {"guid", KQLFunction::datatype_guid},
    {"int", KQLFunction::datatype_int},
    {"long", KQLFunction::datatype_long},
    {"real", KQLFunction::datatype_real},
    {"double", KQLFunction::datatype_real},
    {"timespan", KQLFunction::datatype_timespan},
    {"time", KQLFunction::datatype_timespan},
    {"decimal", KQLFunction::datatype_decimal}};
}

namespace DB
{
std::unique_ptr<IParserKQLFunction> KQLFunctionFactory::get(const String & kql_function)
{
    const auto kql_function_it = KQL_FUNCTIONS.find(kql_function);
    if (kql_function_it == KQL_FUNCTIONS.end())
        return nullptr;

    const auto& kql_function_id = kql_function_it->second;
    switch (kql_function_id)
    {
        case KQLFunction::none:
            return nullptr;

        case KQLFunction::ago:
            return std::make_unique<Ago>();

        case KQLFunction::datetime_add:
            return std::make_unique<DatetimeAdd>();

        case KQLFunction::datetime_part:
            return std::make_unique<DatetimePart>();

        case KQLFunction::datetime_diff:
            return std::make_unique<DatetimeDiff>();

        case KQLFunction::dayofmonth:
            return std::make_unique<DayOfMonth>();

        case KQLFunction::dayofweek:
            return std::make_unique<DayOfWeek>();

        case KQLFunction::dayofyear:
            return std::make_unique<DayOfYear>();

        case KQLFunction::endofday:
            return std::make_unique<EndOfDay>();

        case KQLFunction::endofweek:
            return std::make_unique<EndOfWeek>();

        case KQLFunction::endofyear:
            return std::make_unique<EndOfYear>();

        case KQLFunction::endofmonth:
            return std::make_unique<EndOfMonth>();

        case KQLFunction::monthofyear:
            return std::make_unique<MonthOfYear>();

        case KQLFunction::format_datetime:
            return std::make_unique<FormatDateTime>();

        case KQLFunction::format_timespan:
            return std::make_unique<FormatTimeSpan>();

        case KQLFunction::getmonth:
            return std::make_unique<GetMonth>();

        case KQLFunction::getyear:
            return std::make_unique<GetYear>();

        case KQLFunction::hourofday:
            return std::make_unique<HoursOfDay>();

        case KQLFunction::make_timespan:
            return std::make_unique<MakeTimeSpan>();

        case KQLFunction::make_datetime:
            return std::make_unique<MakeDateTime>();

        case KQLFunction::now:
            return std::make_unique<Now>();

        case KQLFunction::startofday:
            return std::make_unique<StartOfDay>();

        case KQLFunction::startofmonth:
            return std::make_unique<StartOfMonth>();

        case KQLFunction::startofweek:
            return std::make_unique<StartOfWeek>();

        case KQLFunction::startofyear:
            return std::make_unique<StartOfYear>();

        case KQLFunction::unixtime_microseconds_todatetime:
            return std::make_unique<UnixTimeMicrosecondsToDateTime>();

        case KQLFunction::unixtime_milliseconds_todatetime:
            return std::make_unique<UnixTimeMillisecondsToDateTime>();

        case KQLFunction::unixtime_nanoseconds_todatetime:
            return std::make_unique<UnixTimeNanosecondsToDateTime>();

        case KQLFunction::unixtime_seconds_todatetime:
            return std::make_unique<UnixTimeSecondsToDateTime>();

        case KQLFunction::week_of_year:
            return std::make_unique<WeekOfYear>();

        case KQLFunction::base64_encode_tostring:
            return std::make_unique<Base64EncodeToString>();

        case KQLFunction::base64_encode_fromguid:
            return std::make_unique<Base64EncodeFromGuid>();

        case KQLFunction::base64_decode_tostring:
            return std::make_unique<Base64DecodeToString>();

        case KQLFunction::base64_decode_toarray:
            return std::make_unique<Base64DecodeToArray>();

        case KQLFunction::base64_decode_toguid:
            return std::make_unique<Base64DecodeToGuid>();

        case KQLFunction::countof:
            return std::make_unique<CountOf>();

        case KQLFunction::extract:
            return std::make_unique<Extract>();

        case KQLFunction::extract_all:
            return std::make_unique<ExtractAll>();

        case KQLFunction::extract_json:
            return std::make_unique<ExtractJson>();

        case KQLFunction::has_any_index:
            return std::make_unique<HasAnyIndex>();

        case KQLFunction::indexof:
            return std::make_unique<IndexOf>();

        case KQLFunction::isempty:
            return std::make_unique<IsEmpty>();

        case KQLFunction::isnan:
            return std::make_unique<IsNan>();

        case KQLFunction::isnotempty:
            return std::make_unique<IsNotEmpty>();

        case KQLFunction::isnotnull:
            return std::make_unique<IsNotNull>();

        case KQLFunction::isnull:
            return std::make_unique<IsNull>();

        case KQLFunction::parse_command_line:
            return std::make_unique<ParseCommandLine>();

        case KQLFunction::parse_csv:
            return std::make_unique<ParseCSV>();

        case KQLFunction::parse_json:
            return std::make_unique<ParseJson>();

        case KQLFunction::parse_url:
            return std::make_unique<ParseURL>();

        case KQLFunction::parse_urlquery:
            return std::make_unique<ParseURLQuery>();

        case KQLFunction::parse_version:
            return std::make_unique<ParseVersion>();

        case KQLFunction::replace_regex:
            return std::make_unique<ReplaceRegex>();

        case KQLFunction::reverse:
            return std::make_unique<Reverse>();

        case KQLFunction::split:
            return std::make_unique<Split>();

        case KQLFunction::strcat:
            return std::make_unique<StrCat>();

        case KQLFunction::strcat_delim:
            return std::make_unique<StrCatDelim>();

        case KQLFunction::strcmp:
            return std::make_unique<StrCmp>();

        case KQLFunction::strlen:
            return std::make_unique<StrLen>();

        case KQLFunction::strrep:
            return std::make_unique<StrRep>();

        case KQLFunction::substring:
            return std::make_unique<SubString>();

        case KQLFunction::tolower:
            return std::make_unique<ToLower>();

        case KQLFunction::toupper:
            return std::make_unique<ToUpper>();

        case KQLFunction::translate:
            return std::make_unique<Translate>();

        case KQLFunction::trim:
            return std::make_unique<Trim>();

        case KQLFunction::trim_end:
            return std::make_unique<TrimEnd>();

        case KQLFunction::trim_start:
            return std::make_unique<TrimStart>();

        case KQLFunction::url_decode:
            return std::make_unique<URLDecode>();

        case KQLFunction::url_encode:
            return std::make_unique<URLEncode>();

        case KQLFunction::array_concat:
            return std::make_unique<ArrayConcat>();

        case KQLFunction::array_iif:
            return std::make_unique<ArrayIif>();

        case KQLFunction::array_index_of:
            return std::make_unique<ArrayIndexOf>();

        case KQLFunction::array_length:
            return std::make_unique<ArrayLength>();

        case KQLFunction::array_reverse:
            return std::make_unique<ArrayReverse>();

        case KQLFunction::array_rotate_left:
            return std::make_unique<ArrayRotateLeft>();

        case KQLFunction::array_rotate_right:
            return std::make_unique<ArrayRotateRight>();

        case KQLFunction::array_shift_left:
            return std::make_unique<ArrayShiftLeft>();

        case KQLFunction::array_shift_right:
            return std::make_unique<ArrayShiftRight>();

        case KQLFunction::array_slice:
            return std::make_unique<ArraySlice>();

        case KQLFunction::array_sort_asc:
            return std::make_unique<ArraySortAsc>();

        case KQLFunction::array_sort_desc:
            return std::make_unique<ArraySortDesc>();

        case KQLFunction::array_split:
            return std::make_unique<ArraySplit>();

        case KQLFunction::array_sum:
            return std::make_unique<ArraySum>();

        case KQLFunction::bag_keys:
            return std::make_unique<BagKeys>();

        case KQLFunction::bag_merge:
            return std::make_unique<BagMerge>();

        case KQLFunction::bag_remove_keys:
            return std::make_unique<BagRemoveKeys>();

        case KQLFunction::jaccard_index:
            return std::make_unique<JaccardIndex>();

        case KQLFunction::pack:
            return std::make_unique<Pack>();

        case KQLFunction::pack_all:
            return std::make_unique<PackAll>();

        case KQLFunction::pack_array:
            return std::make_unique<PackArray>();

        case KQLFunction::repeat:
            return std::make_unique<Repeat>();

        case KQLFunction::set_difference:
            return std::make_unique<SetDifference>();

        case KQLFunction::set_has_element:
            return std::make_unique<SetHasElement>();

        case KQLFunction::set_intersect:
            return std::make_unique<SetIntersect>();

        case KQLFunction::set_union:
            return std::make_unique<SetUnion>();

        case KQLFunction::treepath:
            return std::make_unique<TreePath>();

        case KQLFunction::zip:
            return std::make_unique<Zip>();

        case KQLFunction::tobool:
            return std::make_unique<ToBool>();

        case KQLFunction::todatetime:
            return std::make_unique<ToDateTime>();

        case KQLFunction::todouble:
            return std::make_unique<ToDouble>();

        case KQLFunction::toint:
            return std::make_unique<ToInt>();

        case KQLFunction::tolong:
            return std::make_unique<ToLong>();

        case KQLFunction::tostring:
            return std::make_unique<ToString>();

        case KQLFunction::totimespan:
            return std::make_unique<ToTimeSpan>();

        case KQLFunction::todecimal:
            return std::make_unique<ToDecimal>();

        case KQLFunction::arg_max:
            return std::make_unique<ArgMax>();

        case KQLFunction::arg_min:
            return std::make_unique<ArgMin>();

        case KQLFunction::avg:
            return std::make_unique<Avg>();

        case KQLFunction::avgif:
            return std::make_unique<AvgIf>();

        case KQLFunction::binary_all_and:
            return std::make_unique<BinaryAllAnd>();

        case KQLFunction::binary_all_or:
            return std::make_unique<BinaryAllOr>();

        case KQLFunction::binary_all_xor:
            return std::make_unique<BinaryAllXor>();

        case KQLFunction::buildschema:
            return std::make_unique<BuildSchema>();

        case KQLFunction::count:
            return std::make_unique<Count>();

        case KQLFunction::countif:
            return std::make_unique<CountIf>();

        case KQLFunction::dcount:
            return std::make_unique<DCount>();

        case KQLFunction::dcountif:
            return std::make_unique<DCountIf>();

        case KQLFunction::make_bag:
            return std::make_unique<MakeBag>();

        case KQLFunction::make_bag_if:
            return std::make_unique<MakeBagIf>();

        case KQLFunction::make_list:
            return std::make_unique<MakeList>();

        case KQLFunction::make_list_if:
            return std::make_unique<MakeListIf>();

        case KQLFunction::make_list_with_nulls:
            return std::make_unique<MakeListWithNulls>();

        case KQLFunction::make_set:
            return std::make_unique<MakeSet>();

        case KQLFunction::make_set_if:
            return std::make_unique<MakeSetIf>();

        case KQLFunction::max:
            return std::make_unique<Max>();

        case KQLFunction::maxif:
            return std::make_unique<MaxIf>();

        case KQLFunction::min:
            return std::make_unique<Min>();

        case KQLFunction::minif:
            return std::make_unique<MinIf>();

        case KQLFunction::percentile:
            return std::make_unique<Percentile>();

        case KQLFunction::percentilew:
            return std::make_unique<Percentilew>();

        case KQLFunction::percentiles:
            return std::make_unique<Percentiles>();

        case KQLFunction::percentiles_array:
            return std::make_unique<PercentilesArray>();

        case KQLFunction::percentilesw:
            return std::make_unique<Percentilesw>();

        case KQLFunction::percentilesw_array:
            return std::make_unique<PercentileswArray>();

        case KQLFunction::stdev:
            return std::make_unique<Stdev>();

        case KQLFunction::stdevif:
            return std::make_unique<StdevIf>();

        case KQLFunction::sum:
            return std::make_unique<Sum>();

        case KQLFunction::sumif:
            return std::make_unique<SumIf>();

        case KQLFunction::take_any:
            return std::make_unique<TakeAny>();

        case KQLFunction::take_anyif:
            return std::make_unique<TakeAnyIf>();

        case KQLFunction::variance:
            return std::make_unique<Variance>();

        case KQLFunction::varianceif:
            return std::make_unique<VarianceIf>();

        case KQLFunction::count_distinct:
            return std::make_unique<CountDistinct>();

        case KQLFunction::count_distinctif:
            return std::make_unique<CountDistinctIf>();


        case KQLFunction::series_fir:
            return std::make_unique<SeriesFir>();

        case KQLFunction::series_iir:
            return std::make_unique<SeriesIir>();

        case KQLFunction::series_fit_line:
            return std::make_unique<SeriesFitLine>();

        case KQLFunction::series_fit_line_dynamic:
            return std::make_unique<SeriesFitLineDynamic>();

        case KQLFunction::series_fit_2lines:
            return std::make_unique<SeriesFit2lines>();

        case KQLFunction::series_fit_2lines_dynamic:
            return std::make_unique<SeriesFit2linesDynamic>();

        case KQLFunction::series_outliers:
            return std::make_unique<SeriesOutliers>();

        case KQLFunction::series_periods_detect:
            return std::make_unique<SeriesPeriodsDetect>();

        case KQLFunction::series_periods_validate:
            return std::make_unique<SeriesPeriodsValidate>();

        case KQLFunction::series_stats_dynamic:
            return std::make_unique<SeriesStatsDynamic>();

        case KQLFunction::series_stats:
            return std::make_unique<SeriesStats>();

        case KQLFunction::series_fill_backward:
            return std::make_unique<SeriesFillBackward>();

        case KQLFunction::series_fill_const:
            return std::make_unique<SeriesFillConst>();

        case KQLFunction::series_fill_forward:
            return std::make_unique<SeriesFillForward>();

        case KQLFunction::series_fill_linear:
            return std::make_unique<SeriesFillLinear>();

        case KQLFunction::ipv4_compare:
            return std::make_unique<Ipv4Compare>();

        case KQLFunction::ipv4_is_in_range:
            return std::make_unique<Ipv4IsInRange>();

        case KQLFunction::ipv4_is_match:
            return std::make_unique<Ipv4IsMatch>();

        case KQLFunction::ipv4_is_private:
            return std::make_unique<Ipv4IsPrivate>();

        case KQLFunction::ipv4_netmask_suffix:
            return std::make_unique<Ipv4NetmaskSuffix>();

        case KQLFunction::parse_ipv4:
            return std::make_unique<ParseIpv4>();

        case KQLFunction::parse_ipv4_mask:
            return std::make_unique<ParseIpv4Mask>();

        case KQLFunction::has_ipv6:
            return std::make_unique<HasIpv6>();

        case KQLFunction::has_any_ipv6:
            return std::make_unique<HasAnyIpv6>();

        case KQLFunction::has_ipv6_prefix:
            return std::make_unique<HasIpv6Prefix>();

        case KQLFunction::has_any_ipv6_prefix:
            return std::make_unique<HasAnyIpv6Prefix>();

        case KQLFunction::ipv6_compare:
            return std::make_unique<Ipv6Compare>();

        case KQLFunction::ipv6_is_match:
            return std::make_unique<Ipv6IsMatch>();

        case KQLFunction::parse_ipv6:
            return std::make_unique<ParseIpv6>();

        case KQLFunction::parse_ipv6_mask:
            return std::make_unique<ParseIpv6Mask>();

        case KQLFunction::format_ipv4:
            return std::make_unique<FormatIpv4>();

        case KQLFunction::format_ipv4_mask:
            return std::make_unique<FormatIpv4Mask>();

        case KQLFunction::binary_and:
            return std::make_unique<BinaryAnd>();

        case KQLFunction::binary_not:
            return std::make_unique<BinaryNot>();

        case KQLFunction::binary_or:
            return std::make_unique<BinaryOr>();

        case KQLFunction::binary_shift_left:
            return std::make_unique<BinaryShiftLeft>();

        case KQLFunction::binary_shift_right:
            return std::make_unique<BinaryShiftRight>();

        case KQLFunction::binary_xor:
            return std::make_unique<BinaryXor>();

        case KQLFunction::bitset_count_ones:
            return std::make_unique<BitsetCountOnes>();

        case KQLFunction::bin:
            return std::make_unique<Bin>();

        case KQLFunction::bin_at:
            return std::make_unique<BinAt>();

        case KQLFunction::kase:
            return std::make_unique<Case>();

        case KQLFunction::iff:
            return std::make_unique<Iff>();

        case KQLFunction::iif:
            return std::make_unique<Iif>();

        case KQLFunction::datatype_bool:
            return std::make_unique<DatatypeBool>();

        case KQLFunction::datatype_datetime:
            return std::make_unique<DatatypeDatetime>();

        case KQLFunction::datatype_dynamic:
            return std::make_unique<DatatypeDynamic>();

        case KQLFunction::datatype_guid:
            return std::make_unique<DatatypeGuid>();

        case KQLFunction::datatype_int:
            return std::make_unique<DatatypeInt>();

        case KQLFunction::datatype_long:
            return std::make_unique<DatatypeLong>();

        case KQLFunction::datatype_real:
            return std::make_unique<DatatypeReal>();

        case KQLFunction::datatype_timespan:
            return std::make_unique<DatatypeTimespan>();

        case KQLFunction::datatype_decimal:
            return std::make_unique<DatatypeDecimal>();
    }
}
}
