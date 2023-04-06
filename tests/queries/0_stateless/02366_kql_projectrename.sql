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

print '1-- rename one column';
Customers | project-rename FN=FirstName;

print '2-- rename two columns';
Customers | project-rename FN=FirstName, LN=LastName;

print '3-- rename printed columns';
print FN='Theodore', LN='Diaz' | project-rename FirstName=FN, LastName=LN;
