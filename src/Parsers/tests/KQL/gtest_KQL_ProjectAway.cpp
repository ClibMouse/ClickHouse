#include <Parsers/tests/gtest_common.h>

#include <Parsers/Kusto/ParserKQLStatement.h>

INSTANTIATE_TEST_SUITE_P(ParserKQLQuery_ProjectAway, ParserTest,
    ::testing::Combine(
        ::testing::Values(std::make_shared<DB::ParserKQLStatement>()),
        ::testing::ValuesIn(std::initializer_list<ParserTestCase>{
        {
            "Customers | project-away FirstName",
            "SELECT * EXCEPT FirstName\nFROM Customers"
        },
        {
            "Customers | project-away FirstName, LastName",
            "SELECT * EXCEPT (FirstName, LastName)\nFROM Customers"
        },
        {
            "Customers | project-away *Name",
            "SELECT * EXCEPT '.*Name'\nFROM Customers"
        },
        {
            "Customers | project-away *Name, *tion",
            "SELECT * EXCEPT '.*Name'\nFROM\n(\n    SELECT * EXCEPT '.*tion'\n    FROM Customers\n)"
        },
        {
            "Customers | project-away *Name, Age",
            "SELECT * EXCEPT Age\nFROM\n(\n    SELECT * EXCEPT '.*Name'\n    FROM Customers\n)"
        },
        {
            "Customers | project-away *Name, Age, Education",
            "SELECT * EXCEPT (Age, Education)\nFROM\n(\n    SELECT * EXCEPT '.*Name'\n    FROM Customers\n)"
        },
        {
            "Customers | project-away *irstName, Age, *astName, Education",
            "SELECT * EXCEPT (Age, Education)\nFROM\n(\n    SELECT * EXCEPT '.*astName'\n    FROM\n    (\n        SELECT * EXCEPT '.*irstName'\n        FROM Customers\n    )\n)"
        },
        {
            "Customers | where Age< 30 | limit 2 | project-away FirstName",
            "SELECT * EXCEPT FirstName\nFROM\n(\n    SELECT *\n    FROM Customers\n    WHERE Age < 30\n    LIMIT 2\n)"
        },
        {
            "Customers|summarize sum(Age), avg(Age) by FirstName | project-away sum_Age",
            "SELECT * EXCEPT sum_Age\nFROM\n(\n    SELECT\n        FirstName,\n        sum(Age) AS sum_Age,\n        avg(Age) AS avg_Age\n    FROM Customers\n    GROUP BY FirstName\n)"
        },
        {
            "Customers|extend FullName = strcat(FirstName,' ',LastName) | project-away FirstName, LastName",
            "SELECT * EXCEPT (FirstName, LastName)\nFROM\n(\n    SELECT\n        * EXCEPT FullName,\n        concat(ifNull(kql_tostring(FirstName), ''), ifNull(kql_tostring(' '), ''), ifNull(kql_tostring(LastName), ''), '') AS FullName\n    FROM Customers\n)"
        }
})));
