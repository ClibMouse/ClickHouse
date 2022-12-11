DROP TABLE IF EXISTS Customers;
CREATE TABLE Customers
(    
    FirstName Nullable(String),
    LastName String, 
    Occupation String,
    Education String,
    Age Nullable(UInt8)
) ENGINE = Memory;

INSERT INTO Customers VALUES  ('Theodore','Diaz','Skilled Manual','Bachelors',28),('Stephanie','Cox','Management abcd defg','Bachelors',33),('Peter','Nara','Skilled Manual','Graduate Degree',26),('Latoya','Shen','Professional','Graduate Degree',25),('Apple','','Skilled Manual','Bachelors',28),(NULL,'why','Professional','Partial College',38);
Select '-- #1 --' ;
select * from kql(Customers | where FirstName !in ('Peter', 'Latoya'));
Select '-- #2 --' ;
select * from kql(Customers | where FirstName !in ("test", "test2"));
Select '-- #3 --' ;
select * from kql(Customers | where FirstName !contains 'Pet');
Select '-- #4 --' ;
select * from kql(Customers | where FirstName !contains_cs 'Pet');
Select '-- #5 --' ;
select * from kql(Customers | where FirstName !endswith 'ter');
Select '-- #6 --' ;
select * from kql(Customers | where FirstName !endswith_cs 'ter');
Select '-- #7 --' ;
select * from kql(Customers | where FirstName != 'Peter');
Select '-- #8 --' ;
select * from kql(Customers | where FirstName !has 'Peter');
Select '-- #9 --' ;
select * from kql(Customers | where FirstName !has_cs 'peter');
Select '-- #10 --' ;
select * from kql(Customers | where FirstName !hasprefix 'Peter');
Select '-- #11 --' ;
select * from kql(Customers | where FirstName !hasprefix_cs 'Peter');
Select '-- #12 --' ;
select * from kql(Customers | where FirstName !hassuffix 'Peter');
Select '-- #13 --' ;
select * from kql(Customers | where FirstName !hassuffix_cs 'Peter');
Select '-- #14 --' ;
select * from kql(Customers | where FirstName !startswith 'Peter');
Select '-- #15 --' ;
select * from kql(Customers | where FirstName !startswith_cs 'Peter');
Select '-- #16 --' ;
select * from kql(print t = 'a' in~ ('A', 'b', 'c'));
Select '-- #17 --' ;
select * from kql(Customers | where FirstName in~ ('peter', 'apple'));
Select '-- #18 --' ;
select * from kql(Customers | where FirstName in~ ((Customers | project FirstName | where FirstName == 'Peter')));
Select '-- #19 --' ;
select * from kql(Customers | where FirstName in~ ((Customers | project FirstName | where Age < 30)));
Select '-- #20 --' ;
select * from kql(print t = 'a' !in~ ('A', 'b', 'c'));
Select '-- #21 --' ;
select * from kql(Customers | where FirstName !in~ ('peter', 'apple'));
Select '-- #22 --' ;
select * from kql(Customers | where FirstName !in~ ((Customers | project FirstName | where FirstName == 'Peter')));
Select '-- #23 --' ;
select * from kql(Customers | where FirstName !in~ ((Customers | project FirstName | where Age < 30)));
Select '-- #24 --' ;
select * from kql(Customers | where FirstName =~ 'peter' and LastName =~ 'naRA');
Select '-- #25 --' ;
select * from kql(Customers | where FirstName !~ 'nEyMaR' and LastName =~ 'naRA');

DROP TABLE IF EXISTS Customers;
