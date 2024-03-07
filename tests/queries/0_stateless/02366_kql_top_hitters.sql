DROP TABLE IF EXISTS Customers;
CREATE TABLE Customers
(    
    FirstName Nullable(String),
    LastName String, 
    Occupation String,
    Education String,
    Age Nullable(UInt8),
    extra Int16
) ENGINE = Memory;

INSERT INTO Customers VALUES ('Theodore','Diaz','Skilled Manual','Bachelors',28,10),('Stephanie','Cox','Management','Bachelors',31,20),('Peter','Nara','Skilled Manual','Graduate Degree',26,30),('Latoya','Shen','Professional','Graduate Degree',25,40),('Joshua','Lee','Professional','Partial College',26,50),('Edward','Hernandez','Skilled Manual','High School',36,60),('Dalton','Wood','Professional','Partial College',42,70),('Christine','Nara','Skilled Manual','Partial College',33,80),('Cameron','Rodriguez','Professional','Partial College',28,90),('Angel','Stewart','Professional','Partial College',46,100),('Apple','B','Skilled Manual','Bachelors',28,110),(NULL,'why','Professional','Partial College',38,120);

set dialect = 'kusto';
print '--top 1--';
Customers | top 3 by Age;
print '--top 2--';
Customers | top 3 by Age desc;
print '--top 3--';
Customers | top 3 by Age asc | order by FirstName;
print '--top 4--';
Customers | top 3 by FirstName  desc nulls first;
print '--top 5--';
Customers | top 3 by FirstName  desc nulls last;
print '--top 6--';
Customers | top 3 by Age | top 2 by FirstName;
print '--top hitters 1--';
Customers | top-hitters a = 2 of Age by extra;
print '--top hitters 2--';
Customers | top-hitters 2 of Age;
print '--top hitters 3--';
Customers | top-hitters 2 of Age by extra | top-hitters 2 of Age | order by Age;
print '--top hitters 4--';
Customers | top-hitters 2 of Age by extra | where Age > 30;
print '--top hitters 5--';
Customers | top-hitters 2 of Age by extra | where approximate_sum_extra < 200;
print '--top hitters 6--';
Customers | top-hitters 2 of Age | where approximate_count_Age > 2;

