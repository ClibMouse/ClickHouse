-- let Customers = datatable (FirstName:string, LastName:string, Occupation:string, Education:string, Age:int) [
--     'Theodore', 'Diaz', 'Skilled Manual', 'Bachelors', 28,
--     'Stephanie', 'Cox', 'Management', 'Bachelors', 33,
--     'Peter', 'Nara', 'Skilled Manual', 'Graduate Degree', 26,
--     'Latoya', 'Shen', 'Professional', 'Graduate Degree', 25,
--     'Joshua', 'Lee', 'Professional', 'Partial College', 26,
--     'Edward', 'Hernandez', 'Skilled Manual', 'High School', 36,
--     'Dalton', 'Wood', 'Professional', 'Partial College', 42,
--     'Christine', 'Nara', 'Skilled Manual', 'Partial College', 33,
--     'Cameron', 'Rodriguez', 'Professional', 'Partial College', 28,
--     'Angel', 'Stewart', 'Professional', 'Partial College', 46
-- ];

DROP TABLE IF EXISTS Customers;
CREATE TABLE Customers
(    
    FirstName Nullable(String),
    LastName String, 
    Occupation String,
    Education String,
    Age Nullable(UInt8)
) ENGINE = Memory;

INSERT INTO Customers VALUES ('Theodore','Diaz','Skilled Manual','Bachelors',28), ('Stephanie','Cox','Management','Bachelors',33), ('Peter','Nara','Skilled Manual','Graduate Degree',26), ('Latoya','Shen','Professional','Graduate Degree',25), ('Joshua','Lee','Professional','Partial College',26), ('Edward','Hernandez','Skilled Manual','High School',36), ('Dalton','Wood','Professional','Partial College',42), ('Christine','Nara','Skilled Manual','Partial College',33), ('Cameron','Rodriguez','Professional','Partial College',28), ('Angel','Stewart','Professional','Partial College',46);

DROP TABLE IF EXISTS dictionary_source_table;
CREATE TABLE dictionary_source_table
(
    key String,
    start_range UInt64,
    end_range UInt64,
    value String,
    value_nullable Nullable(String)
)
ENGINE = Memory;
INSERT INTO dictionary_source_table VALUES('1', 10, 20, 'First', 'First'), ('2', 11, 21, 'Second', NULL), ('3', 12, 22, 'Third', 'Third');

drop dictionary if exists dictionary_table;
CREATE DICTIONARY dictionary_table
(
    key String,
    start_range UInt64,
    end_range UInt64,
    value String,
    value_nullable Nullable(String)
)
PRIMARY KEY key
SOURCE(CLICKHOUSE(HOST 'localhost' PORT tcpPort() TABLE 'dictionary_source_table'))
LIFETIME(MIN 1 MAX 1000)
LAYOUT(FLAT());

set dialect='kusto';

print '-- case';
Customers | extend t = case(Age <= 10, "A", Age <= 20, "B", Age <= 30, "C", "D");
print '-- iff';
Customers | extend t = iff(Age <= 10, "smaller", "bigger");
print '-- iif';
Customers | extend t = iif(Age <= 10, "smaller", "bigger");
print '-- lookup';
print lookup('dictionary_table', 'value', '1');
print lookup('dictionary_table', 'value', '100', 'default');
dictionary_source_table | project start_range, t = lookup('dictionary_table', 'value', '1'), key;
print '-- gettype';
Customers | project t = gettype(FirstName) | limit 1;
Customers | project t = gettype(Age) | limit 1;
print t = gettype(range(1, 10));
print t = gettype(todatetime('2023-09-08'));

print '-- toscalar #1 --';
print x = 5 | extend a = toscalar(print 5, 'asd' | project y = strcat(print_0, print_1));
print '-- toscalar #2 --';
range z from toscalar(print x=1) to toscalar(range x from 1 to 9 step 1 | count) step toscalar(2);
print '-- toscalar #3 --';
range x from 1 to 2 step 1 | extend x=toscalar(print new_guid()), y=new_guid() | count;
print '-- toscalar #4 --';
range x from 1 to 2 step 1 | extend x=toscalar(new_guid()), y=new_guid() | distinct x | count;
print '-- toscalar #5 --';
Customers | where FirstName == toscalar(Customers | where Age > 30 | order by Age asc, FirstName);
print '-- toscalar #6 --';
Customers | order by Age | limit toscalar(Customers | where Age == 33 | count);
print '-- toscalar #7 --';
Customers | where Age == toscalar(print 33, 'asd') | count;
print '-- toscalar #8 --';
Customers | limit toscalar(Customers | where Age > toscalar(toscalar(print 5, 'asd')) | count) | count;

print '-- not --';
print t = not(1);
print t = not(false);
print t = not(strlen('abc'));
print t = not(1.1); -- result differs from ADX
Customers | project not(Age);