DROP TABLE IF EXISTS Customers;
CREATE TABLE Customers
(    
    FirstName Nullable(String),
    LastName String, 
    Occupation String,
    Education String,
    Age Nullable(UInt8)
) ENGINE = Memory;

INSERT INTO Customers VALUES ('Theodore','Diaz','Skilled Manual','Bachelors',28), ('Stephanie','Cox','Management abcd defg','Bachelors',33),('Peter','Nara','Skilled Manual','Graduate Degree',26),('Latoya','Shen','Professional','Graduate Degree',25),('Apple','','Skilled Manual','Bachelors',28),(NULL,'why','Professional','Partial College',38);

set dialect='kusto';

print '-- support subquery for in orerator (https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/in-cs-operator) (subquery need to be wraped with bracket inside bracket); TODO: case-insensitive not supported yet';
Customers | where Age in ((Customers|project Age|where Age < 30)) | order by LastName;

