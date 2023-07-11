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

print  '4-- nested query';
print a = 9 | project-rename b = a | extend c = toscalar(print d = 8 | project-rename e = d) | project-rename f = c;

print '5-- rename duplicate column';
print a='a', b='b', c='c' | project-rename c=b; -- { serverError SYNTAX_ERROR }
