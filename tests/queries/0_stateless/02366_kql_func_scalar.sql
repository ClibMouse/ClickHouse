-- datatable (Date:datetime, Num:int) [
--     '2018-02-24T15:14:00', 3,
--     '2018-02-23T16:14:00', 4,
--     '2018-02-26T15:14:00', 5
-- ] | summarize sum(Num) by d = todatetime(bin_at(Date, 1d, datetime('2018-02-24 15:14:00'))) | order by d;

DROP TABLE IF EXISTS Bin_at_test;
CREATE TABLE Bin_at_test
(    
    `Date` DateTime64(9, 'UTC'),
    Num Nullable(UInt8)
) ENGINE = Memory;
INSERT INTO Bin_at_test VALUES ('2018-02-24T15:14:00', 3), ('2018-02-23T16:14:00', 4), ('2018-02-26T15:14:00', 5);

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

set dialect = 'kusto';
print '-- bin()';
print bin(4.5, 1);
print floor(4.5, 3);
print bin(datetime(1970-05-11 13:45:07), 1d);
print bin(16d, 7d);
print bin(datetime(1970-05-11 13:45:07.345623), 1ms);
print bin(datetime(2022-09-26 10:13:23.987234), 6ms);
print bin(datetime(1970-05-11 13:45:07.345623), 1microsecond);
print bin(datetime(2022-09-26 10:13:23.987234), 6microseconds);
print bin(datetime(1970-05-11 13:45:07.456345672), 16microseconds);
print bin(datetime('1970-05-11 13:45:07.456345672'), 1tick);
print bin(datetime(2022-09-26 10:13:23.987234128), 100nanosecond);
print bin(datetime(2022-09-26 10:13:23.987234128), 1tick);
print bin(datetime(2022-09-26 10:13:23.987234128), 99nanosecond);
Customers | summarize count() by bin(Age, Age) | sort by Age asc;

print '-- bin_at()';
print bin_at(6.5, 2.5, 7);
print bin_at(1h, 1d, 12h);
print bin_at(datetime(2017-05-15 10:20:00.0), 1d, datetime(1970-01-01 12:00:00.0));
print bin_at(datetime(2017-05-17 10:20:00.0), 7d, datetime(2017-06-04 00:00:00.0));
Bin_at_test | summarize sum(Num) by d = todatetime(bin_at(Date, 1d, datetime('2018-02-24 15:14:00'))) | order by d;
