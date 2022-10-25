#include <Parsers/tests/gtest_common.h>

#include <Parsers/Kusto/ParserKQLQuery.h>

INSTANTIATE_TEST_SUITE_P(ParserKQLQuery_TopHitters, ParserTest,
    ::testing::Combine(
        ::testing::Values(std::make_shared<DB::ParserKQLQuery>()),
        ::testing::ValuesIn(std::initializer_list<ParserTestCase>{
        {
            "Customers | top 5 by Age",
            "SELECT *\nFROM Customers\nORDER BY Age DESC\nLIMIT 5"
        },
        {
            "Customers | top 5 by Age desc",
            "SELECT *\nFROM Customers\nORDER BY Age DESC\nLIMIT 5"
        },
        {
            "Customers | top 5 by Age asc",
            "SELECT *\nFROM Customers\nORDER BY Age ASC\nLIMIT 5"
        },
        {
            "Customers | top 5 by FirstName  desc nulls first",
            "SELECT *\nFROM Customers\nORDER BY FirstName DESC NULLS FIRST\nLIMIT 5"
        },
        {
            "Customers | top 5 by FirstName  desc nulls last",
            "SELECT *\nFROM Customers\nORDER BY FirstName DESC NULLS LAST\nLIMIT 5"
        },
        {
            "Customers | top 5 by Age | top 2 by FirstName",
            "SELECT *\nFROM\n(\n    SELECT *\n    FROM Customers\n    ORDER BY Age DESC\n    LIMIT 5\n)\nORDER BY FirstName DESC\nLIMIT 2"
        },
        {
            "Customers|  top-hitters a = 3 of Age by extra",
            "SELECT *\nFROM\n(\n    SELECT\n        Age,\n        sum(extra) AS approximate_sum_extra\n    FROM Customers\n    GROUP BY Age\n)\nORDER BY approximate_sum_extra DESC\nLIMIT 3 AS a"
        },
        {
            "Customers|  top-hitters  3 of Age",
            "SELECT *\nFROM\n(\n    SELECT\n        Age,\n        count() AS approximate_count_Age\n    FROM Customers\n    GROUP BY Age\n)\nORDER BY approximate_count_Age DESC\nLIMIT 3"
        },
        {
            "Customers|  top-hitters  3 of Age by extra | top-hitters 2 of Age",
            "SELECT *\nFROM\n(\n    SELECT\n        Age,\n        count() AS approximate_count_Age\n    FROM\n    (\n        SELECT *\n        FROM\n        (\n            SELECT\n                Age,\n                sum(extra) AS approximate_sum_extra\n            FROM Customers\n            GROUP BY Age\n        )\n        ORDER BY approximate_sum_extra DESC\n        LIMIT 3\n    )\n    GROUP BY Age\n)\nORDER BY approximate_count_Age DESC\nLIMIT 2"
        },
        {
            "Customers|  top-hitters  3 of Age by extra | where Age > 30",
            "SELECT *\nFROM\n(\n    SELECT *\n    FROM\n    (\n        SELECT\n            Age,\n            sum(extra) AS approximate_sum_extra\n        FROM Customers\n        GROUP BY Age\n    )\n    ORDER BY approximate_sum_extra DESC\n    LIMIT 3\n)\nWHERE Age > 30"
        },
        {
            "Customers|  top-hitters  3 of Age by extra | where approximate_sum_extra < 200",
            "SELECT *\nFROM\n(\n    SELECT *\n    FROM\n    (\n        SELECT\n            Age,\n            sum(extra) AS approximate_sum_extra\n        FROM Customers\n        GROUP BY Age\n    )\n    ORDER BY approximate_sum_extra DESC\n    LIMIT 3\n)\nWHERE approximate_sum_extra < 200"
        },
        {
            "Customers|  top-hitters  3 of Age | where approximate_count_Age > 2",
            "SELECT *\nFROM\n(\n    SELECT *\n    FROM\n    (\n        SELECT\n            Age,\n            count() AS approximate_count_Age\n        FROM Customers\n        GROUP BY Age\n    )\n    ORDER BY approximate_count_Age DESC\n    LIMIT 3\n)\nWHERE approximate_count_Age > 2"
        }
})));
