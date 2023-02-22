#include <Parsers/tests/gtest_common.h>

#include <Parsers/Kusto/ParserKQLQuery.h>
#include <Parsers/ParserSelectQuery.h>

INSTANTIATE_TEST_SUITE_P(ParserKQLQuery_operator_in_sql, ParserKQLTest,
    ::testing::Combine(
        ::testing::Values(std::make_shared<DB::ParserSelectQuery>()),
        ::testing::ValuesIn(std::initializer_list<ParserTestCase>{
        {
            "select * from kql($$Customers | where FirstName !in ('Peter', 'Latoya')$$)",
            "SELECT *\nFROM view(\n    SELECT *\n    FROM Customers\n    WHERE FirstName NOT IN ('Peter', 'Latoya')\n)"
        },
        {
            "select * from kql($$Customers | where FirstName !contains 'Pet'$$);",
            "SELECT *\nFROM view(\n    SELECT *\n    FROM Customers\n    WHERE NOT (FirstName ILIKE '%Pet%')\n)"
        },
        {
            "select * from kql($$Customers | where FirstName !contains_cs 'Pet'$$);",
            "SELECT *\nFROM view(\n    SELECT *\n    FROM Customers\n    WHERE NOT (FirstName LIKE '%Pet%')\n)"
        },
        {
            "select * from kql($$Customers | where FirstName !endswith 'ter'$$);",
            "SELECT *\nFROM view(\n    SELECT *\n    FROM Customers\n    WHERE NOT (FirstName ILIKE '%ter')\n)"
        },
        {
            "select * from kql($$Customers | where FirstName !endswith_cs 'ter'$$);"
            "SELECT *\nFROM view(\n    SELECT *\n    FROM Customers\n    WHERE NOT endsWith(FirstName, 'ter')\n)"
        },
        {
            "select * from kql($$Customers | where FirstName != 'Peter'$$);",
            "SELECT *\nFROM view(\n    SELECT *\n    FROM Customers\n    WHERE FirstName != 'Peter'\n)"
        },
        {
            "select * from kql(Customers | where FirstName !has 'Peter');",
            "SELECT *\nFROM\n(\n    SELECT *\n    FROM Customers\n    WHERE NOT ifNull(hasTokenCaseInsensitiveOrNull(FirstName, 'Peter'), hasTokenCaseInsensitive(FirstName, 'Peter') AND (positionCaseInsensitive(FirstName, 'Peter') > 0))\n)"
        },
        {
            "select * from kql(Customers | where FirstName !has_cs 'peter');",
            "SELECT *\nFROM\n(\n    SELECT *\n    FROM Customers\n    WHERE NOT ifNull(hasTokenOrNull(FirstName, 'peter'), hasToken(FirstName, 'peter') AND (position(FirstName, 'peter') > 0))\n)"
        },
        {
            "select * from kql($$Customers | where FirstName !hasprefix 'Peter'$$);",
            "SELECT *\nFROM view(\n    SELECT *\n    FROM Customers\n    WHERE (NOT (FirstName ILIKE 'Peter%')) AND (NOT (FirstName ILIKE '% Peter%'))\n)"
        },
        {
            "select * from kql($$Customers | where FirstName !hasprefix_cs 'Peter'$$);",
            "SELECT *\nFROM view(\n    SELECT *\n    FROM Customers\n    WHERE (NOT startsWith(FirstName, 'Peter')) AND (NOT (FirstName LIKE '% Peter%'))\n)"
        },
        {
            "select * from kql($$Customers | where FirstName !hassuffix 'Peter'$$);",
            "SELECT *\nFROM view(\n    SELECT *\n    FROM Customers\n    WHERE (NOT (FirstName ILIKE '%Peter')) AND (NOT (FirstName ILIKE '%Peter %'))\n)"
        },
        {
            "select * from kql($$Customers | where FirstName !hassuffix_cs 'Peter'$$);",
            "SELECT *\nFROM view(\n    SELECT *\n    FROM Customers\n    WHERE (NOT endsWith(FirstName, 'Peter')) AND (NOT (FirstName LIKE '%Peter %'))\n)"
        },
        {
            "select * from kql($$Customers | where FirstName !startswith 'Peter'$$);",
            "SELECT *\nFROM view(\n    SELECT *\n    FROM Customers\n    WHERE NOT (FirstName ILIKE 'Peter%')\n)"
        },
        {
            "select * from kql(Customers | where FirstName !startswith_cs 'Peter');",
            "SELECT *\nFROM\n(\n    SELECT *\n    FROM Customers\n    WHERE NOT startsWith(FirstName, 'Peter')\n)"
        },
        {
            "select * from kql(print t = 'a' in~ ('A', 'b', 'c'))",
            "SELECT *\nFROM\n(\n    SELECT lower('a') IN (lower('A'), lower('b'), lower('c')) AS t\n)"
        },
        {
            "select * from kql(Customers | where FirstName in~ ('peter', 'apple'))",
            "SELECT *\nFROM\n(\n    SELECT *\n    FROM Customers\n    WHERE lower(FirstName) IN (lower('peter'), lower('apple'))\n)"
        },
        {
            "select * from kql(Customers | where FirstName in~ ((Customers | project FirstName | where FirstName == 'Peter')))",
            "SELECT *\nFROM\n(\n    SELECT *\n    FROM Customers\n    WHERE lower(FirstName) IN (\n        SELECT lower(FirstName)\n        FROM Customers\n        WHERE FirstName = 'Peter'\n    )\n)"
        },
        {
            "select * from kql(Customers | where FirstName in~ ((Customers | project FirstName | where Age < 30)))",
            "SELECT *\nFROM\n(\n    SELECT *\n    FROM Customers\n    WHERE lower(FirstName) IN (\n        SELECT lower(FirstName)\n        FROM Customers\n        WHERE Age < 30\n    )\n)"
        },
        {
            "select * from kql(print t = 'a' !in~ ('A', 'b', 'c'))",
            "SELECT *\nFROM\n(\n    SELECT lower('a') NOT IN (lower('A'), lower('b'), lower('c')) AS t\n)"
        },
        {
            "select * from kql(print t = 'a' !in~ (dynamic(['A', 'b', 'c'])))",
            "SELECT *\nFROM\n(\n    SELECT lower('a') NOT IN (lower('A'), lower('b'), lower('c')) AS t\n)"
        },
        {
            "select * from kql(Customers | where FirstName !in~ ('peter', 'apple'))",
            "SELECT *\nFROM\n(\n    SELECT *\n    FROM Customers\n    WHERE lower(FirstName) NOT IN (lower('peter'), lower('apple'))\n)"
        },
        {
            "select * from kql(Customers | where FirstName !in~ ((Customers | project FirstName | where FirstName == 'Peter')))",
            "SELECT *\nFROM\n(\n    SELECT *\n    FROM Customers\n    WHERE lower(FirstName) NOT IN (\n        SELECT lower(FirstName)\n        FROM Customers\n        WHERE FirstName = 'Peter'\n    )\n)"
        },
        {
            "select * from kql(Customers | where FirstName !in~ ((Customers | project FirstName | where Age < 30)))",
            "SELECT *\nFROM\n(\n    SELECT *\n    FROM Customers\n    WHERE lower(FirstName) NOT IN (\n        SELECT lower(FirstName)\n        FROM Customers\n        WHERE Age < 30\n    )\n)"
        },
        {
            "select * from kql(Customers | where FirstName =~ 'peter' and LastName =~ 'naRA')",
            "SELECT *\nFROM\n(\n    SELECT *\n    FROM Customers\n    WHERE (lower(FirstName) = lower('peter')) AND (lower(LastName) = lower('naRA'))\n)"
        },
        {
            "select * from kql(Customers | where FirstName !~ 'nEyMaR' and LastName =~ 'naRA')",
            "SELECT *\nFROM\n(\n    SELECT *\n    FROM Customers\n    WHERE (lower(FirstName) != lower('nEyMaR')) AND (lower(LastName) = lower('naRA'))\n)"
        }
})));
