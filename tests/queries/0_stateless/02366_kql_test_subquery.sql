DROP TABLE IF EXISTS Customers;
CREATE TABLE Customers
(    
    FirstName Nullable(String),
    LastName String, 
    Occupation String,
    Education String,
    Age Nullable(UInt8)
) ENGINE = Memory;

INSERT INTO Customers VALUES  ('Theodore','Diaz','Skilled Manual','Bachelors',28),('Peter','Cox','Management abcd defg','Bachelors',33);

set dialect = 'kusto';
print '--  test negetivate operator in kql subuquery --' ;
print '-- #1 --' ;
Customers | where FirstName in ((Customers | project FirstName  | where FirstName !in ('Peter', 'Latoya')));
print '-- #2 --' ;
Customers | where FirstName in ((Customers | project FirstName, Age | where Age !in (28, 29)));
print '-- #3 --' ;
Customers | where FirstName in ((Customers | project FirstName  | where FirstName !contains 'ste'));
print '-- #4 --' ;
Customers | where FirstName in ((Customers | project FirstName  | where FirstName !contains_cs 'Ste'));
print '-- #5 --' ;
Customers | where FirstName in ((Customers | project FirstName  | where FirstName !contains_cs 'ste'));
print '-- #6 --' ;
Customers | where FirstName in ((Customers | project FirstName  | where FirstName !endswith 'ore'));
print '-- #7 --' ;
Customers | where FirstName in ((Customers | project FirstName  | where FirstName !endswith_cs 'Ore'));
Customers | where FirstName in ((Customers | project FirstName  | where FirstName !endswith_cs 'ore'));
print '-- #8 --' ;
Customers | where FirstName in ((Customers | project FirstName  | where FirstName != 'Theodore'));
Customers | where FirstName in ((Customers | project FirstName  | where FirstName !~ 'theodore'));
print '-- #9 --' ;
Customers | where FirstName in ((Customers | project FirstName  | where FirstName !has 'Peter'));
print '-- #10 --' ;
Customers | where FirstName in ((Customers | project FirstName  | where FirstName !has_cs 'Peter'));
print '-- #11 --' ;
Customers | where FirstName in ((Customers | project FirstName  | where FirstName !has_cs 'peter'));
print '-- #12 --' ;
Customers | where FirstName in ((Customers | project FirstName  | where FirstName !hasprefix 'Peter'));
print '-- #13 --' ;
Customers | where FirstName in ((Customers | project FirstName  | where FirstName !hasprefix_cs 'Peter'));
print '-- #14 --' ;
Customers | where FirstName in ((Customers | project FirstName  | where FirstName !hasprefix_cs 'peter'));
print '-- #15 --' ;
Customers | where FirstName in ((Customers | project FirstName  | where FirstName !hassuffix 'Peter'));
print '-- #16 --' ;
Customers | where FirstName in ((Customers | project FirstName  | where FirstName !hassuffix_cs 'Peter'));
print '-- #17 --' ;
Customers | where FirstName in ((Customers | project FirstName  | where FirstName !hassuffix_cs 'peter'));
print '-- #18 --' ;
Customers | where FirstName in ((Customers | project FirstName  | where FirstName !startswith 'Peter'));
print '-- #19 --' ;
Customers | where FirstName in ((Customers | project FirstName  | where FirstName !startswith_cs 'Peter'));
print '-- #20 --' ;
Customers | where FirstName in ((Customers | project FirstName  | where FirstName !startswith_cs 'peter'));
print '';
print '--  test case-insensitive in operator kql subuquery --' ;
print '-- #21 --' ;
Customers | where FirstName !in~ ((Customers | project FirstName  | where FirstName !in~ ('peter', 'apple')));
print '-- #22 --' ;
Customers | where FirstName in ((Customers | project FirstName | where FirstName in~ ('peter', 'apple')));
print '-- #23 --' ;
Customers | where FirstName in ((Customers | project FirstName | where FirstName in~ ((Customers | project FirstName  | where FirstName == 'Peter'))));
print '-- #24 --' ;
Customers | where FirstName in ((Customers | project FirstName | where FirstName in~ ((Customers | project FirstName, Age | where Age < 30))));
print '-- #25 --' ;
Customers | where substring(FirstName,0,3) in~ ((Customers | project substring(FirstName,0,3) | where FirstName in~ ('peter', 'apple')));
print '-- #26 --' ;
Customers | where FirstName in ((Customers | project FirstName | where FirstName !in~ ('peter', 'apple')));
print '-- #27 --' ;
Customers | where FirstName in ((Customers |where Age <30 | project FirstName  | where FirstName !in~ ((Customers | project FirstName  | where FirstName =~ 'peter'))));
print '-- #28 --' ;
Customers | where FirstName in ((Customers | project FirstName | where FirstName !in~ ((Customers | project FirstName, Age |  where Age < 30))));
print '-- #29 --' ;
Customers | where FirstName in~ ((Customers | project FirstName  | where FirstName !in~ ('peter', 'apple')));
print '-- #30 --' ;
Customers | where FirstName in~ ((Customers |  where FirstName !in~ ('peter', 'apple')| project FirstName));
print '';
print '--  test multi columns in operator kql subuquery --' ;
print '-- #32 --' ;
Customers | where FirstName in ((Customers | project FirstName, LastName, Age));
print '-- #33 --' ;
Customers | where FirstName in~ ((Customers | project FirstName, LastName, Age|where Age <30));
print '-- #34 --' ;
Customers | where FirstName !in ((Customers | project FirstName, LastName, Age |where Age <30 ));
print '-- #35 --' ;
Customers | where FirstName !in~ ((Customers | project FirstName, LastName, Age |where Age <30));