#include <Parsers/tests/gtest_common.h>
#include <Parsers/Kusto/ParserKQLStatement.h>

INSTANTIATE_TEST_SUITE_P(ParserKQLQuery_Aggregate, ParserKQLTest,
    ::testing::Combine(
        ::testing::Values(std::make_shared<DB::ParserKQLStatement>()),
        ::testing::ValuesIn(std::initializer_list<ParserTestCase>{
        {
            "Customers | summarize t = stdev(Age) by FirstName",
            "SELECT\n    FirstName,\n    sqrt(varSamp(Age)) AS t\nFROM Customers\nGROUP BY FirstName"
        },
        {
            "Customers | summarize t = stdevif(Age, Age < 10) by FirstName",
            "SELECT\n    FirstName,\n    sqrt(varSampIf(Age, Age < 10)) AS t\nFROM Customers\nGROUP BY FirstName"
        },
        {
            "Customers | summarize t = binary_all_and(Age) by FirstName",
            "SELECT\n    FirstName,\n    groupBitAnd(Age) AS t\nFROM Customers\nGROUP BY FirstName"
        },
        {
            "Customers | summarize t = binary_all_or(Age) by FirstName",
            "SELECT\n    FirstName,\n    groupBitOr(Age) AS t\nFROM Customers\nGROUP BY FirstName"

        },
        {
            "Customers | summarize t = binary_all_xor(Age) by FirstName",
            "SELECT\n    FirstName,\n    groupBitXor(Age) AS t\nFROM Customers\nGROUP BY FirstName"
        },
        {
            "Customers | summarize percentiles(Age, 30, 40, 50, 60, 70) by FirstName",
            "SELECT\n    FirstName,\n    quantiles(30 / 100, 40 / 100, 50 / 100, 60 / 100, 70 / 100)(Age) AS percentiles_Age\nFROM Customers\nGROUP BY FirstName"
        },
        {
            "Customers | summarize t = percentiles_array(Age, 10, 20, 30, 50) by FirstName",
            "SELECT\n    FirstName,\n    quantiles(10 / 100, 20 / 100, 30 / 100, 50 / 100)(Age) AS t\nFROM Customers\nGROUP BY FirstName"
        },
        {
            "Customers | summarize t = percentiles_array(Age, dynamic([10, 20, 30, 50])) by FirstName",
            "SELECT\n    FirstName,\n    quantiles(10 / 100, 20 / 100, 30 / 100, 50 / 100)(Age) AS t\nFROM Customers\nGROUP BY FirstName"
        },
        {
            "DataTable | summarize t = percentilesw(Bucket, Frequency, 50, 75, 99.9)",
            "SELECT quantilesExactWeighted(50 / 100, 75 / 100, 99.9 / 100)(Bucket, Frequency) AS t\nFROM DataTable"
        },
        {
            "DataTable| summarize t = percentilesw_array(Bucket, Frequency, dynamic([10, 50, 30]))",
            "SELECT quantilesExactWeighted(10 / 100, 50 / 100, 30 / 100)(Bucket, Frequency) AS t\nFROM DataTable"
        },
        {
            "Customers | summarize t = percentile(Age, 50) by FirstName",
            "SELECT\n    FirstName,\n    quantile(50 / 100)(Age) AS t\nFROM Customers\nGROUP BY FirstName"
        },
        {
            "DataTable | summarize t = percentilew(Bucket, Frequency, 50)",
            "SELECT quantileExactWeighted(50 / 100)(Bucket, Frequency) AS t\nFROM DataTable"
        },
        {
             "Customers | summarize t = make_list_with_nulls(Age) by FirstName",
             "SELECT\n    FirstName,\n    arrayConcat(groupArray(Age), arrayMap(x -> NULL, range(0, toUInt32(count(*) - length(groupArray(Age))), 1))) AS t\nFROM Customers\nGROUP BY FirstName"
        },
        {
            "Customers | summarize count() by bin(Age, 10)",
            "SELECT\n    kql_bin(Age, 10) AS Age,\n    count() AS count_\nFROM Customers\nGROUP BY Age"
        },
        {
            "Customers | summarize count(Age+1) by bin(Age+1, 10)",
            "SELECT\n    kql_bin(Age + 1, 10) AS Columns1,\n    count(Age + 1) AS count_\nFROM Customers\nGROUP BY Columns1"
        },
        {
            "Customers | summarize count(Age) by bin(Age, 10)",
            "SELECT\n    kql_bin(Age, 10) AS Age,\n    count(Age) AS count_Age\nFROM Customers\nGROUP BY Age"
        },
        {
            "Customers | summarize count_distinct(Education)",
            "SELECT countDistinct(Education) AS Columns1\nFROM Customers"
        },
        {
            "Customers | summarize count_distinctif(Education,Age >30)",
            "SELECT countIfDistinct(Education, Age > 30) AS Columns1\nFROM Customers"
        },
        {
            "Customers | summarize take_any(FirstName)"
            "SELECT any(FirstName) AS take_any_FirstName\nFROM Customers"
        },
        {
            "Customers | summarize take_any(FirstName), take_any(LastName)"
            "SELECT\n    any(FirstName) AS take_any_FirstName,\n    any(LastName) AS take_any_LastName\nFROM Customers"
        },
        {
            "Customers | summarize take_any(FirstName, LastName) by FirstName, LastName"
            "SELECT\n    FirstName,\n    LastName,\n    any(FirstName),\n    any(LastName) AS take_any_FirstName\nFROM Customers\nGROUP BY\n    FirstName,\n    LastName"
        },
        {
            "Customers | summarize take_anyif(FirstName, LastName has 'Diaz')"
            "SELECT anyIf(FirstName, hasTokenCaseInsensitive(LastName, 'Diaz')) AS take_anyif_FirstName\nFROM Customers"
        },
        {
            "Customers | summarize take_anyif(FirstName, LastName has 'Diaz'), dcount(FirstName)"
            "SELECT\n    anyIf(FirstName, hasTokenCaseInsensitive(LastName, 'Diaz')) AS take_anyif_FirstName,\n    countDistinct(FirstName) AS dcount_FirstName\nFROM Customers"
	    },
	    {
            "Customers | summarize dcount(Education, 2)"
            "SELECT countDistinct(Education, 2) AS dcount_Education\nFROM Customers"
	    },
	    {
            "Customers | summarize dcountif(Education, Occupation=='Professional', 2)"
            "SELECT countDistinct(Education, Occupation = 'Professional', 2) AS dcountif_Education\nFROM Customers"
        },
        {	
            "Customers | summarize by FirstName, LastName, Age",
            "SELECT\n    FirstName,\n    LastName,\n    Age\nFROM Customers\nGROUP BY\n    FirstName,\n    LastName,\n    Age"
        },
        {
            "Customers | summarize variance(Age)",
            "SELECT varSamp(Age) AS variance_Age\nFROM Customers"
        },
        {
            "Customers | summarize variancep(Age)",
            "SELECT varPop(Age) AS variancep_Age\nFROM Customers"
        },
        {
            "Customers | summarize varianceif(Age, Age < 30)",
            "SELECT varSampIf(Age, Age < 30) AS varianceif_Age\nFROM Customers"
        },
        {
            "Customers | summarize z=arg_max(Age, FirstName, LastName) by Occupation",
            "SELECT\n    Occupation,\n    argMax(FirstName, Age) AS FirstName,\n    argMax(LastName, Age) AS LastName,\n    argMax(Age, Age) AS z\nFROM Customers\nGROUP BY Occupation"
        },
        {
            "Customers | summarize z=arg_min(Age, FirstName, LastName) by Occupation",
            "SELECT\n    Occupation,\n    argMin(FirstName, Age) AS FirstName,\n    argMin(LastName, Age) AS LastName,\n    argMin(Age, Age) AS z\nFROM Customers\nGROUP BY Occupation"
        }
})));
