#include <Parsers/tests/gtest_common.h>

#include <Parsers/Kusto/ParserKQLStatement.h>

INSTANTIATE_TEST_SUITE_P(ParserKQLQuery_Operators, ParserTest,
    ::testing::Combine(
        ::testing::Values(std::make_shared<DB::ParserKQLStatement>()),
        ::testing::ValuesIn(std::initializer_list<ParserTestCase>{
        {
            "Customers",
            "SELECT *\nFROM Customers"
        },
        {
            "Customers | project FirstName,LastName,Occupation",
            "SELECT\n    FirstName,\n    LastName,\n    Occupation\nFROM Customers"
        },
        {
            "Customers | project FirstName,LastName,Occupation | take 3",
            "SELECT\n    FirstName,\n    LastName,\n    Occupation\nFROM Customers\nLIMIT 3"
        },
        {
            "Customers | project FirstName,LastName,Occupation | limit 3",
            "SELECT\n    FirstName,\n    LastName,\n    Occupation\nFROM Customers\nLIMIT 3"
        },
        {
            "Customers | project FirstName,LastName,Occupation | take 1 | take 3",
            "SELECT *\nFROM\n(\n    SELECT\n        FirstName,\n        LastName,\n        Occupation\n    FROM Customers\n    LIMIT 1\n)\nLIMIT 3"
        },
        {
            "Customers | project FirstName,LastName,Occupation | take 3 | take 1",
            "SELECT *\nFROM\n(\n    SELECT\n        FirstName,\n        LastName,\n        Occupation\n    FROM Customers\n    LIMIT 3\n)\nLIMIT 1"
        },
        {
            "Customers | project FirstName,LastName,Occupation | take 3 | project FirstName,LastName",
            "SELECT\n    FirstName,\n    LastName\nFROM\n(\n    SELECT\n        FirstName,\n        LastName,\n        Occupation\n    FROM Customers\n    LIMIT 3\n)"
        },
        {
            "Customers | sort by FirstName desc",
            "SELECT *\nFROM Customers\nORDER BY FirstName DESC NULLS LAST"
        },
        {
            "Customers | take 3 | order by FirstName desc",
            "SELECT *\nFROM\n(\n    SELECT *\n    FROM Customers\n    LIMIT 3\n)\nORDER BY FirstName DESC NULLS LAST"
        },
        {
            "Customers | sort by FirstName asc",
            "SELECT *\nFROM Customers\nORDER BY FirstName ASC NULLS FIRST"
        },
        {
            "Customers | sort by FirstName",
            "SELECT *\nFROM Customers\nORDER BY FirstName DESC NULLS LAST"
        },
        {
            "Customers | order by Age desc, FirstName asc",
            "SELECT *\nFROM Customers\nORDER BY\n    Age DESC NULLS LAST,\n    FirstName ASC NULLS FIRST"
        },
        {
            "Customers | order by Age asc , FirstName desc",
            "SELECT *\nFROM Customers\nORDER BY\n    Age ASC NULLS FIRST,\n    FirstName DESC NULLS LAST"
        },
        {
            "Customers | sort by FirstName | order by Age ",
            "SELECT *\nFROM Customers\nORDER BY\n    Age DESC NULLS LAST,\n    FirstName DESC NULLS LAST"
        },
        {
            "Customers | sort by FirstName nulls first",
            "SELECT *\nFROM Customers\nORDER BY FirstName DESC NULLS FIRST"
        },
        {
            "Customers | sort by FirstName nulls last",
            "SELECT *\nFROM Customers\nORDER BY FirstName DESC NULLS LAST"
        },
        {
            "Customers | where Occupation == 'Skilled Manual'",
            "SELECT *\nFROM Customers\nWHERE Occupation = 'Skilled Manual'"
        },
        {
            "Customers | where Occupation != 'Skilled Manual'",
            "SELECT *\nFROM Customers\nWHERE Occupation != 'Skilled Manual'"
        },
        {
            "Customers |where Education in  ('Bachelors','High School')",
            "SELECT *\nFROM Customers\nWHERE Education IN ('Bachelors', 'High School')"
        },
        {
            "Customers |  where Education !in  ('Bachelors','High School')",
            "SELECT *\nFROM Customers\nWHERE Education NOT IN ('Bachelors', 'High School')"
        },
        {
            "Customers |where Education contains_cs  'Degree'",
            "SELECT *\nFROM Customers\nWHERE Education LIKE '%Degree%'"
        },
        {
            "Customers | where Occupation startswith_cs  'Skil'",
            "SELECT *\nFROM Customers\nWHERE startsWith(Occupation, 'Skil')"
        },
        {
            "Customers | where FirstName endswith_cs  'le'",
            "SELECT *\nFROM Customers\nWHERE endsWith(FirstName, 'le')"
        },
        {
            "Customers | where Age == 26",
            "SELECT *\nFROM Customers\nWHERE Age = 26"
        },
        {
            "Customers | where Age > 20 and Age < 30",
            "SELECT *\nFROM Customers\nWHERE (Age > 20) AND (Age < 30)"
        },
        {
            "Customers | where Age > 30 | where Education == 'Bachelors'",
            "SELECT *\nFROM Customers\nWHERE (Education = 'Bachelors') AND (Age > 30)"
        },
        {
            "Customers |summarize count() by Occupation",
            "SELECT\n    Occupation,\n    count() AS count_\nFROM Customers\nGROUP BY Occupation"
        },
        {
            "Customers|summarize sum(Age) by Occupation",
            "SELECT\n    Occupation,\n    sum(Age) AS sum_Age\nFROM Customers\nGROUP BY Occupation"
        },
        {
            "Customers|summarize  avg(Age) by Occupation",
            "SELECT\n    Occupation,\n    avg(Age) AS avg_Age\nFROM Customers\nGROUP BY Occupation"
        },
        {
            "Customers|summarize  min(Age) by Occupation",
            "SELECT\n    Occupation,\n    min(Age) AS min_Age\nFROM Customers\nGROUP BY Occupation"
        },
        {
            "Customers |summarize  max(Age) by Occupation",
            "SELECT\n    Occupation,\n    max(Age) AS max_Age\nFROM Customers\nGROUP BY Occupation"
        },
        {
            "Customers | where FirstName contains 'pet'",
            "SELECT *\nFROM Customers\nWHERE FirstName ILIKE '%pet%'"
        },
        {
            "Customers | where FirstName !contains 'pet'",
            "SELECT *\nFROM Customers\nWHERE NOT (FirstName ILIKE '%pet%')"
        },
        {
            "Customers | where FirstName endswith 'er'",
            "SELECT *\nFROM Customers\nWHERE FirstName ILIKE '%er'"
        },
        {
            "Customers | where FirstName !endswith 'er'",
            "SELECT *\nFROM Customers\nWHERE NOT (FirstName ILIKE '%er')"
        },
        {
            "Customers | where FirstName matches regex 'P.*r'",
            "SELECT *\nFROM Customers\nWHERE match(FirstName, 'P.*r')"
        },
        {
            "Customers | where FirstName startswith 'pet'",
            "SELECT *\nFROM Customers\nWHERE FirstName ILIKE 'pet%'"
        },
        {
            "Customers | where FirstName !startswith 'pet'",
            "SELECT *\nFROM Customers\nWHERE NOT (FirstName ILIKE 'pet%')"
        },
        {
            "Customers | where Age in ((Customers|project Age|where Age < 30))",
            "SELECT *\nFROM Customers\nWHERE Age IN (\n    SELECT Age\n    FROM Customers\n    WHERE Age < 30\n)"
        },
        {
            "Customers | where Education has 'School'",
            "SELECT *\nFROM Customers\nWHERE ifNull(hasTokenCaseInsensitiveOrNull(Education, 'School'), hasTokenCaseInsensitive(Education, 'School') AND (positionCaseInsensitive(Education, 'School') > 0))"
        },
        {
            "Customers | where Education !has 'School'",
            "SELECT *\nFROM Customers\nWHERE NOT ifNull(hasTokenCaseInsensitiveOrNull(Education, 'School'), hasTokenCaseInsensitive(Education, 'School') AND (positionCaseInsensitive(Education, 'School') > 0))"
        },
        {
            "Customers | where Education has_cs 'School'",
            "SELECT *\nFROM Customers\nWHERE ifNull(hasTokenOrNull(Education, 'School'), hasToken(Education, 'School') AND (position(Education, 'School') > 0))"
        },
        {
            "Customers | where Education !has_cs 'School'",
            "SELECT *\nFROM Customers\nWHERE NOT ifNull(hasTokenOrNull(Education, 'School'), hasToken(Education, 'School') AND (position(Education, 'School') > 0))"
        },
        {
            "Customers|where Occupation has_any ('Skilled','abcd')",
            "SELECT *\nFROM Customers\nWHERE ifNull(hasTokenCaseInsensitiveOrNull(Occupation, 'Skilled'), hasTokenCaseInsensitive(Occupation, 'Skilled') AND (positionCaseInsensitive(Occupation, 'Skilled') > 0)) OR ifNull(hasTokenCaseInsensitiveOrNull(Occupation, 'abcd'), hasTokenCaseInsensitive(Occupation, 'abcd') AND (positionCaseInsensitive(Occupation, 'abcd') > 0))"
        },
        {
            "Customers|where Occupation has_all ('Skilled','abcd')",
            "SELECT *\nFROM Customers\nWHERE ifNull(hasTokenCaseInsensitiveOrNull(Occupation, 'Skilled'), hasTokenCaseInsensitive(Occupation, 'Skilled') AND (positionCaseInsensitive(Occupation, 'Skilled') > 0)) AND ifNull(hasTokenCaseInsensitiveOrNull(Occupation, 'abcd'), hasTokenCaseInsensitive(Occupation, 'abcd') AND (positionCaseInsensitive(Occupation, 'abcd') > 0))"
        },
        {
            "Customers|where Occupation has_all (strcat('Skill','ed'),'Manual')",
            "SELECT *\nFROM Customers\nWHERE ifNull(hasTokenCaseInsensitiveOrNull(Occupation, concat(ifNull(kql_tostring('Skill'), ''), ifNull(kql_tostring('ed'), ''), '')), hasTokenCaseInsensitive(Occupation, 'concat') AND hasTokenCaseInsensitive(Occupation, 'ifNull') AND hasTokenCaseInsensitive(Occupation, 'kql') AND hasTokenCaseInsensitive(Occupation, 'tostring') AND hasTokenCaseInsensitive(Occupation, 'Skill') AND hasTokenCaseInsensitive(Occupation, 'ifNull') AND hasTokenCaseInsensitive(Occupation, 'kql') AND hasTokenCaseInsensitive(Occupation, 'tostring') AND hasTokenCaseInsensitive(Occupation, 'ed') AND (positionCaseInsensitive(Occupation, concat(ifNull(kql_tostring('Skill'), ''), ifNull(kql_tostring('ed'), ''), '')) > 0)) AND ifNull(hasTokenCaseInsensitiveOrNull(Occupation, 'Manual'), hasTokenCaseInsensitive(Occupation, 'Manual') AND (positionCaseInsensitive(Occupation, 'Manual') > 0))"
        },
        {
            "Customers | where Occupation == strcat('Pro','fessional') | take 1",
            "SELECT *\nFROM Customers\nWHERE Occupation = concat(ifNull(kql_tostring('Pro'), ''), ifNull(kql_tostring('fessional'), ''), '')\nLIMIT 1"
        },
        {
            "Customers | project countof('The cat sat on the mat', 'at')",
            "SELECT countSubstrings('The cat sat on the mat', 'at')\nFROM Customers"
        },
        {
            "Customers | project countof('The cat sat on the mat', 'at', 'normal')",
            "SELECT countSubstrings('The cat sat on the mat', 'at')\nFROM Customers"
        },
        {
            "Customers | project countof('The cat sat on the mat', 'at', 'regex')",
            "SELECT countMatches('The cat sat on the mat', 'at')\nFROM Customers"
        },
        {
            "Customers | project extract('(\\b[A-Z]+\\b).+(\\b\\d+)', 0, 'The price of PINEAPPLE ice cream is 10')",
            "SELECT kql_extract('The price of PINEAPPLE ice cream is 10', '(\\b[A-Z]+\\b).+(\\b\\\\d+)', 0)\nFROM Customers"
        },
        {
            "Customers | project extract('(\\b[A-Z]+\\b).+(\\b\\d+)', 1, 'The price of PINEAPPLE ice cream is 20')",
            "SELECT kql_extract('The price of PINEAPPLE ice cream is 20', '(\\b[A-Z]+\\b).+(\\b\\\\d+)', 1)\nFROM Customers"
        },
        {
            "Customers | project extract('(\\b[A-Z]+\\b).+(\\b\\d+)', 2, 'The price of PINEAPPLE ice cream is 30')",
            "SELECT kql_extract('The price of PINEAPPLE ice cream is 30', '(\\b[A-Z]+\\b).+(\\b\\\\d+)', 2)\nFROM Customers"
        },
        {
            "Customers | project extract('(\\b[A-Z]+\\b).+(\\b\\d+)', 2, 'The price of PINEAPPLE ice cream is 40', typeof(int))",
            "SELECT accurateCastOrNull(kql_extract('The price of PINEAPPLE ice cream is 40', '(\\b[A-Z]+\\b).+(\\b\\\\d+)', 2), 'Int32')\nFROM Customers"
        },
        {
            "Customers | project extract_all('(\\w)(\\w+)(\\w)','The price of PINEAPPLE ice cream is 50')",
            "SELECT extractAllGroups('The price of PINEAPPLE ice cream is 50', '(\\\\w)(\\\\w+)(\\\\w)')\nFROM Customers"
        },
        {
            " Customers | project split('aa_bb', '_')",
            "SELECT if(empty('_'), splitByString(' ', 'aa_bb'), splitByString('_', 'aa_bb'))\nFROM Customers"
        },
        {
            "Customers | project split('aaa_bbb_ccc', '_', 1)",
            "SELECT multiIf((length(if(empty('_'), splitByString(' ', 'aaa_bbb_ccc'), splitByString('_', 'aaa_bbb_ccc'))) >= 2) AND (2 > 0), arrayPushBack([], if(empty('_'), splitByString(' ', 'aaa_bbb_ccc'), splitByString('_', 'aaa_bbb_ccc'))[2]), 2 = 0, if(empty('_'), splitByString(' ', 'aaa_bbb_ccc'), splitByString('_', 'aaa_bbb_ccc')), arrayPushBack([], NULL[1]))\nFROM Customers"
        },
        {
            "Customers | project strcat_delim('-', '1', '2', 'A')",
            "SELECT concat(ifNull(kql_tostring('1'), ''), '-', ifNull(kql_tostring('2'), ''), '-', ifNull(kql_tostring('A'), ''))\nFROM Customers"
        },
        {
            "print x=1, s=strcat('Hello', ', ', 'World!')",
            "SELECT\n    1 AS x,\n    concat(ifNull(kql_tostring('Hello'), ''), ifNull(kql_tostring(', '), ''), ifNull(kql_tostring('World!'), ''), '') AS s"
        },
        {
            "print parse_urlquery('https://john:123@google.com:1234/this/is/a/path?k1=v1&k2=v2#fragment')",
            "SELECT concat('{', concat('\"Query Parameters\":', concat('{\"', replace(replace(if(position('https://john:123@google.com:1234/this/is/a/path?k1=v1&k2=v2#fragment', '?') > 0, queryString('https://john:123@google.com:1234/this/is/a/path?k1=v1&k2=v2#fragment'), 'https://john:123@google.com:1234/this/is/a/path?k1=v1&k2=v2#fragment'), '=', '\":\"'), '&', '\",\"'), '\"}')), '}') AS print_0"
        },
        {
            "print strcmp('a','b')",
            "SELECT multiIf('a' = 'b', 0, 'a' < 'b', -1, 1) AS print_0"
        },
        {
            "Customers | summarize t = make_list(FirstName) by FirstName",
            "SELECT\n    FirstName,\n    groupArrayIf(FirstName, FirstName IS NOT NULL) AS t\nFROM Customers\nGROUP BY FirstName"
        },
        {
            "Customers | summarize t = make_list(FirstName, 10) by FirstName",
            "SELECT\n    FirstName,\n    groupArrayIf(10)(FirstName, FirstName IS NOT NULL) AS t\nFROM Customers\nGROUP BY FirstName"
        },
        {
            "Customers | summarize t = make_list_if(FirstName, Age > 10) by FirstName",
            "SELECT\n    FirstName,\n    groupArrayIf(FirstName, Age > 10) AS t\nFROM Customers\nGROUP BY FirstName"
        },
        {
            "Customers | summarize t = make_list_if(FirstName, Age > 10, 10) by FirstName",
            "SELECT\n    FirstName,\n    groupArrayIf(10)(FirstName, Age > 10) AS t\nFROM Customers\nGROUP BY FirstName"
        },
        {
            "Customers | summarize t = make_set(FirstName) by FirstName",
            "SELECT\n    FirstName,\n    groupUniqArray(FirstName) AS t\nFROM Customers\nGROUP BY FirstName"
        },
        {
            "Customers | summarize t = make_set(FirstName, 10) by FirstName",
            "SELECT\n    FirstName,\n    groupUniqArray(10)(FirstName) AS t\nFROM Customers\nGROUP BY FirstName"
        },
        {
            "Customers | summarize t = make_set_if(FirstName, Age > 10) by FirstName",
            "SELECT\n    FirstName,\n    groupUniqArrayIf(FirstName, Age > 10) AS t\nFROM Customers\nGROUP BY FirstName"
        },
        {
            "Customers | summarize t = make_set_if(FirstName, Age > 10, 10) by FirstName",
            "SELECT\n    FirstName,\n    groupUniqArrayIf(10)(FirstName, Age > 10) AS t\nFROM Customers\nGROUP BY FirstName"
        },
        {
            "print output = dynamic([1, 2, 3])",
            "SELECT [1, 2, 3] AS output"
        },
        {
            "print output = dynamic(['a', 'b', 'c'])",
            "SELECT ['a', 'b', 'c'] AS output"
        },
        {
            "T | extend duration = endTime - startTime",
            "SELECT\n    * EXCEPT duration,\n    endTime - startTime AS duration\nFROM T"
        },
        {
            "T |project endTime, startTime | extend duration = endTime - startTime",
            "SELECT\n    * EXCEPT duration,\n    endTime - startTime AS duration\nFROM\n(\n    SELECT\n        endTime,\n        startTime\n    FROM T\n)"
        },
        {
            "T | extend c =c*2, b-a, d = a +b , a*b",
            "SELECT\n    * EXCEPT (c, Column1, d, Column2),\n    c * 2 AS c,\n    b - a AS Column1,\n    a + b AS d,\n    a * b AS Column2\nFROM T"
        },
        {
            "print 5, 4 | extend Column3 = 'a', 'b', 'c' | extend 'd'",
            "SELECT\n    * EXCEPT Column4,\n    'd' AS Column4\nFROM\n(\n    SELECT\n        * EXCEPT (Column3, Column1, Column2),\n        'a' AS Column3,\n        'b' AS Column1,\n        'c' AS Column2\n    FROM\n    (\n        SELECT\n            5 AS print_0,\n            4 AS print_1\n    )\n)"
        }
})));
