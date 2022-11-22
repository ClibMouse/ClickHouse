#include <Parsers/tests/gtest_common.h>

#include <Parsers/Kusto/ParserKQLQuery.h>

INSTANTIATE_TEST_SUITE_P(ParserKQLQuery_lookup_join, ParserTest,
    ::testing::Combine(
        ::testing::Values(std::make_shared<DB::ParserKQLQuery>()),
        ::testing::ValuesIn(std::initializer_list<ParserTestCase>{
        {
            "FactTable | lookup kind=leftouter DimTable on Personal, Family",
            "SELECT *\nFROM FactTable AS left_\nLEFT JOIN DimTable AS right_ USING (Personal, Family)"
        },
        {
            "FactTable | lookup kind=inner  DimTable on Personal, Family",
            "SELECT *\nFROM FactTable AS left_\nINNER JOIN DimTable AS right_ USING (Personal, Family)"
        },
        {
            "FactTable | lookup kind=leftouter (DimTable | where Personal == 'Bill') on Personal, Family",
            "SELECT *\nFROM FactTable AS left_\nLEFT JOIN\n(\n    SELECT *\n    FROM DimTable\n    WHERE Personal = 'Bill'\n) AS right_ USING (Personal, Family)"
        },
        {
            "FactTable | project Personal , Family| lookup kind=leftouter DimTable on Personal, Family",
            "SELECT *\nFROM\n(\n    SELECT\n        Personal,\n        Family\n    FROM FactTable\n) AS left_\nLEFT JOIN DimTable AS right_ USING (Personal, Family)"
        },
        {
            "FactTable | lookup kind=leftouter DimTable on $left.Personal == $right.Personal, $left.Family == $right.Family",
            "SELECT *\nFROM FactTable AS left_\nLEFT JOIN DimTable AS right_ USING (Personal, Family)"
        },
        {
            "FactTable | lookup kind=leftouter DimTable on Personal , $left.Family == $right.Family",
            "SELECT *\nFROM FactTable AS left_\nLEFT JOIN DimTable AS right_ USING (Personal, Family)"
        },
        {
            "FactTable|lookup kind=leftouter DimTable on Personal , ($left.Family == $right.Family)",
            "SELECT *\nFROM FactTable AS left_\nLEFT JOIN DimTable AS right_ USING (Personal, Family)"
        },
        {
            "FactTable | project Row, Personal , Family | lookup kind=leftouter (FactTable | lookup kind=leftouter DimTable on Personal) on Personal, Family",
            "SELECT *\nFROM\n(\n    SELECT\n        Row,\n        Personal,\n        Family\n    FROM FactTable\n) AS left_\nLEFT JOIN\n(\n    SELECT *\n    FROM FactTable AS left_\n    LEFT JOIN DimTable AS right_ USING (Personal)\n) AS right_ USING (Personal, Family)"
        },
        {
            "FactTable|project Row, Personal , Family| lookup kind=leftouter (DimTable | where Personal == 'Bill') on Personal, Family| lookup kind=inner DimTable on Personal, Family",
            "SELECT *\nFROM\n(\n    SELECT *\n    FROM\n    (\n        SELECT\n            Row,\n            Personal,\n            Family\n        FROM FactTable\n    ) AS left_\n    LEFT JOIN\n    (\n        SELECT *\n        FROM DimTable\n        WHERE Personal = 'Bill'\n    ) AS right_ USING (Personal, Family)\n) AS left_\nINNER JOIN DimTable AS right_ USING (Personal, Family)"
        },
        {
            "FactTable| lookup kind=leftouter DimTable on $left.Personal == $right.FirstName, Family",
            "SELECT *\nFROM FactTable AS left_\nLEFT JOIN DimTable AS right_ ON (left_.Personal = right_.FirstName) AND (left_.Family = right_.Family)"
        },
        {
            "FactTable| lookup kind=leftouter DimTable on $left.Personal == $right.FirstName, Family| lookup kind=inner DimTable on $left.Personal == $right.FirstName",
            "SELECT *\nFROM\n(\n    SELECT *\n    FROM FactTable AS left_\n    LEFT JOIN DimTable AS right_ ON (left_.Personal = right_.FirstName) AND (left_.Family = right_.Family)\n) AS left_\nINNER JOIN DimTable AS right_ ON left_.Personal = right_.FirstName"
        },
        {
            "X | join Y on Key",
            "SELECT *\nFROM\n(\n    SELECT *\n    FROM X\n    LIMIT 1 BY Key\n) AS left_\nINNER JOIN Y AS right_ USING (Key)"
        },
        {
            "X | join kind=innerunique Y on Key",
            "SELECT *\nFROM\n(\n    SELECT *\n    FROM X\n    LIMIT 1 BY Key\n) AS left_\nINNER JOIN Y AS right_ USING (Key)"
        },
        {
            "X | join kind=inner Y on Key",
            "SELECT *\nFROM X AS left_\nINNER JOIN Y AS right_ USING (Key)"
        },
        {
            "X | join kind=leftouter Y on Key",
            "SELECT *\nFROM X AS left_\nLEFT JOIN Y AS right_ USING (Key)"
        },
        {
            "X | join kind=rightouter Y on Key",
            "SELECT *\nFROM X AS left_\nRIGHT JOIN Y AS right_ USING (Key)"
        },
        {
            "X | join kind=fullouter Y on Key",
            "SELECT *\nFROM X AS left_\nFULL OUTER JOIN Y AS right_ USING (Key)"
        },
        {
            "X | join kind=leftanti Y on Key",
            "SELECT *\nFROM X AS left_\nANTI LEFT JOIN Y AS right_ USING (Key)"
        },
        {
            "X | join kind=rightanti Y on Key",
            "SELECT *\nFROM X AS left_\nANTI RIGHT JOIN Y AS right_ USING (Key)"
        },
        {
            "X | join kind=leftsemi Y on Key",
            "SELECT *\nFROM X AS left_\nSEMI LEFT JOIN Y AS right_ USING (Key)"
        },
        {
            "X | join kind=rightsemi Y on Key",
            "SELECT *\nFROM X AS left_\nSEMI RIGHT JOIN Y AS right_ USING (Key)"
        },
        {
            "X | join Y on $left.Key == $right.Key",
            "SELECT *\nFROM\n(\n    SELECT *\n    FROM X\n    LIMIT 1 BY Key\n) AS left_\nINNER JOIN Y AS right_ USING (Key)"
        },
        {
            "X | join Y on $left.Key == $right.Key2",
            "SELECT *\nFROM\n(\n    SELECT *\n    FROM X\n    LIMIT 1 BY Key\n) AS left_\nINNER JOIN Y AS right_ ON left_.Key = right_.Key2"
        },
        {
            "X | join (Y | project Key, value2) on $left.Key == $right.Key",
            "SELECT *\nFROM\n(\n    SELECT *\n    FROM X\n    LIMIT 1 BY Key\n) AS left_\nINNER JOIN\n(\n    SELECT\n        Key,\n        value2\n    FROM Y\n) AS right_ USING (Key)"
        }

})));
