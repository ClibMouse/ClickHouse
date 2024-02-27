#include <Parsers/tests/gtest_common.h>

#include <Parsers/Kusto/ParserKQLStatement.h>

INSTANTIATE_TEST_SUITE_P(ParserKQLQuery_TimeSeries, ParserKQLTest,
    ::testing::Combine(
        ::testing::Values(std::make_shared<DB::ParserKQLStatement>()),
        ::testing::ValuesIn(std::initializer_list<ParserTestCase>{
        {
            "print series_decompose_anomalies(dynamic([1,2,4,55,56,8]))",
            "SELECT seriesAnomaliesDetectDecompose([1, 2, 4, 55, 56, 8], 1.5, -1, 'ctukey') AS print_0"
        },
        {
            "print series_decompose_anomalies(dynamic([4, 3, 2, 4, 3, 2, 4, 3, 2, 4, 3, 2, 4, 3, 2, 4, 3, 2, 4, 3, 2]))",
            "SELECT seriesAnomaliesDetectDecompose([4, 3, 2, 4, 3, 2, 4, 3, 2, 4, 3, 2, 4, 3, 2, 4, 3, 2, 4, 3, 2], 1.5, -1, 'ctukey') AS print_0"
        },
        {
            "print series_decompose_anomalies(dynamic([1,2,4,55,56,8]),1.5,-1,'linefit',0,'tukey')",
            "SELECT seriesAnomaliesDetectDecompose([1, 2, 4, 55, 56, 8], 1.5, -1, 'tukey') AS print_0"
        },
        {
            "print series_decompose(dynamic([10.1, 20.45, 40.34, 10.1, 20.45, 40.34, 10.1, 20.45, 40.34]),-1)",
            "SELECT seriesDecomposeSTL([10.1, 20.45, 40.34, 10.1, 20.45, 40.34, 10.1, 20.45, 40.34], seriesPeriodDetectFFT([10.1, 20.45, 40.34, 10.1, 20.45, 40.34, 10.1, 20.45, 40.34])) AS print_0"
        },
        {
            "print series_decompose(dynamic([10.1, 20.45, 40.34, 10.1, 20.45, 40.34, 10.1, 20.45, 40.34]),3)",
            "SELECT seriesDecomposeSTL([10.1, 20.45, 40.34, 10.1, 20.45, 40.34, 10.1, 20.45, 40.34], 3) AS print_0"
        },
        {
            "print series_periods_detect(dynamic([10.1, 20.45, 40.34, 10.1, 20.45, 40.34, 10.1, 20.45, 40.34]))",
            "SELECT seriesPeriodDetectFFT([10.1, 20.45, 40.34, 10.1, 20.45, 40.34, 10.1, 20.45, 40.34]) AS print_0"
        },
        {
            "print series_outliers(dynamic([-3, 2, 15, 3, 5, 6, 4.50, 5, 12, 45, 12, 3.40, 3, 4, 5, 6]))",
            "SELECT seriesOutliersDetectTukey([-3, 2, 15, 3, 5, 6, 4.5, 5, 12, 45, 12, 3.4, 3, 4, 5, 6], 10, 90, 1.5) AS print_0"
        },
        {
            "print series_outliers(dynamic([-3, 2, 15, 3, 5, 6, 4.50, 5, 12, 45, 12, 3.40, 3, 4, 5, 6]), 'tukey',0,20,80)",
            "SELECT seriesOutliersDetectTukey([-3, 2, 15, 3, 5, 6, 4.5, 5, 12, 45, 12, 3.4, 3, 4, 5, 6], 20, 80, 1.5) AS print_0"
        },
        {
            "print series_outliers(dynamic([-3, 2, 15, 3, 5, 6, 4.50, 5, 12, 45, 12, 3.40, 3, 4, 5, 6]), 'ctukey',0,20,80)",
            "SELECT seriesOutliersDetectTukey([-3, 2, 15, 3, 5, 6, 4.5, 5, 12, 45, 12, 3.4, 3, 4, 5, 6], 20, 80, 1.5) AS print_0"
        }
        
})));
