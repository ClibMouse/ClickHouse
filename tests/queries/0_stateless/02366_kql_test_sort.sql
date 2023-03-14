DROP TABLE IF EXISTS Customers;
CREATE TABLE Customers
(    
    FirstName Nullable(String),
    LastName String, 
    Occupation String,
    Education String,
    Age Nullable(UInt8)
) ENGINE = Memory;

INSERT INTO Customers VALUES ('Theodore','Diaz','Skilled Manual','Bachelors',28), ('Stephanie','Cox','Management abcd defg','Bachelors',33),('Peter','Nara','Skilled Manual','Graduate Degree',26),('Latoya','Shen','Professional','Graduate Degree',25),('Apple','','Skilled Manual','Bachelors',NULL),(NULL,'why','Professional','Partial College',38);

set dialect = 'kusto';
print '--1--';
Customers | order by FirstName;
print '--2--';
Customers | order by FirstName asc;
print '--3--';
Customers | order by FirstName desc;
print '--4--';
Customers | order by FirstName asc nulls first;
print '--5--';
Customers | order by FirstName asc nulls last;
print '--6--';
Customers | order by FirstName desc nulls first;
print '--7--';
Customers | order by FirstName desc nulls last;
print '--8--';
Customers | order by FirstName nulls first;
print '--9--';
Customers | order by FirstName nulls last;
print '--10--';
Customers | order by FirstName, Age;
print '--11--';
Customers | order by FirstName asc, Age desc;
print '--12--';
Customers | order by FirstName desc, Age asc ;
print '--13--';
Customers | order by FirstName asc nulls first, Age asc nulls first;
print '--14--';
Customers | order by FirstName asc nulls last, Age asc nulls last;
print '--15--';
Customers | order by FirstName desc nulls first, Age desc nulls first;
print '--16--';
Customers | order by FirstName desc nulls last, Age desc nulls last;
print '--17--';
Customers | order by FirstName nulls first, Age nulls first;
print '--18--';
Customers | order by FirstName nulls last, Age nulls last;
print '--19--';
Customers | order by FirstName , Age asc nulls last, LastName nulls first;
print '--20--';
Customers | order by strcat(FirstName, ' ',LastName),  Age asc nulls last, LastName nulls first;
