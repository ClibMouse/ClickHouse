DROP TABLE IF EXISTS Customers;
CREATE TABLE Customers
(    
    FirstName Nullable(String),
    LastName String, 
    Occupation String,
    Education String,
    Age Nullable(UInt8)
) ENGINE = Memory;

INSERT INTO Customers VALUES ('Theodore','Diaz','Skilled Manual','Bachelors',28);

set dialect = 'kusto';
print '1-- remove one column';
Customers | project-away FirstName;
print '';
print '2-- remove two columns';
Customers | project-away FirstName, LastName;
print '';
print '3-- remove columns by one wildcard';
Customers | project-away *Name;
print '';
print '4-- remove columns by two wildcards';
Customers | project-away *Name, *tion;
print '';
print '5-- remove columns by one wildcard, one regular column';
Customers | project-away *Name, Age;
print '';
print '6-- remove columns by one wildcard, two regular column';
Customers | project-away *Name, Age, Education;
print '';
print '7-- remove columns by two wildcard, two regular column';
Customers | project-away *irstName, Age, *astName, Education;
print '';
print '8-- remove one column from previous piple result';
Customers | where Age< 30 | limit 2 | project-away FirstName;
print '';
print '9-- remove one column from summized piple result';
Customers|summarize sum(Age), avg(Age) by FirstName | project-away sum_Age;
print '';
print '10-- remove columns after extend';
Customers|extend FullName = strcat(FirstName,' ',LastName) | project-away FirstName, LastName;
