-- datatable(FirstName:string, LastName:string, Occupation:string, Education:string, Age:int) [
--     'Theodore', 'Diaz', 'Skilled Manual', 'Bachelors', 28, 
--     'Stephanie', 'Cox', 'Management abcd defg', 'Bachelors', 33, 
--     'Peter', 'Nara', 'Skilled Manual', 'Graduate Degree', 26, 
--     'Latoya', 'Shen', 'Professional', 'Graduate Degree', 25, 
--     'Joshua', 'Lee', 'Professional', 'Partial College', 26, 
--     'Edward', 'Hernandez', 'Skilled Manual', 'High School', 36, 
--     'Dalton', 'Wood', 'Professional', 'Partial College', 42, 
--     'Christine', 'Nara', 'Skilled Manual', 'Partial College', 33, 
--     'Cameron', 'Rodriguez', 'Professional', 'Partial College', 28, 
--     'Angel', 'Stewart', 'Professional', 'Partial College', 46, 
--     'Apple', '', 'Skilled Manual', 'Bachelors', 28, 
--     dynamic(null), 'why', 'Professional', 'Partial College', 38
-- ]

DROP TABLE IF EXISTS Customers;
CREATE TABLE Customers
(    
    FirstName Nullable(String),
    LastName String, 
    Occupation String,
    Education String,
    Age Nullable(UInt8)
) ENGINE = Memory;

INSERT INTO Customers VALUES  ('Theodore','Diaz','Skilled Manual','Bachelors',28),('Stephanie','Cox','Management abcd defg','Bachelors',33),('Peter','Nara','Skilled Manual','Graduate Degree',26),('Latoya','Shen','Professional','Graduate Degree',25),('Joshua','Lee','Professional','Partial College',26),('Edward','Hernandez','Skilled Manual','High School',36),('Dalton','Wood','Professional','Partial College',42),('Christine','Nara','Skilled Manual','Partial College',33),('Cameron','Rodriguez','Professional','Partial College',28),('Angel','Stewart','Professional','Partial College',46),('Apple','','Skilled Manual','Bachelors',28),(NULL,'why','Professional','Partial College',38);

-- datatable (LogEntry:string, Created:long) [
--     'Darth Vader has entered the room.', 546,
--     'Rambo is suspciously looking at Darth Vader.', 245234,
--     'Darth Sidious electrocutes both using Force Lightning.', 245554
-- ]

drop table if exists EventLog;
create table EventLog
(
    LogEntry String,
    Created Int64
) ENGINE = Memory;

insert into EventLog values ('Darth Vader has entered the room.', 546), ('Rambo is suspciously looking at Darth Vader.', 245234), ('Darth Sidious electrocutes both using Force Lightning.', 245554);

drop table if exists Dates;
create table Dates
(
    EventTime DateTime('UTC'),
) ENGINE = Memory;

insert into Dates values ('2015-10-12'), ('2016-10-12');

select '-- test summarize --';
set dialect='kusto';
Customers | summarize count(), min(Age), max(Age), avg(Age), sum(Age);
Customers | summarize count(), min(Age), max(Age), avg(Age), sum(Age) by Occupation | order by Occupation;
Customers | summarize countif(Age>40) by Occupation | order by Occupation;
Customers | summarize MyMax = maxif(Age, Age<40) by Occupation | order by Occupation;
Customers | summarize MyMin = minif(Age, Age<40) by Occupation | order by Occupation;
Customers | summarize MyAvg = avgif(Age, Age<40) by Occupation | order by Occupation;
Customers | summarize MySum = sumif(Age, Age<40) by Occupation | order by Occupation;
Customers | summarize dcount(Education);
Customers | summarize dcountif(Education, Occupation=='Professional');
Customers | summarize count_ = count() by bin(Age, 10) | order by count_ asc;
Customers | summarize job_count = count() by Occupation | where job_count > 0 | order by Occupation;
Customers | summarize 'Edu Count'=count() by Education | sort by 'Edu Count' desc; -- { clientError 62 }

print '-- make_list() --';
Customers | summarize f_list = make_list(Education) by Occupation | sort by Occupation;
Customers | summarize f_list = make_list(Education, 2) by Occupation | sort by Occupation;
print '-- make_list_if() --';
Customers | summarize f_list = make_list_if(FirstName, Age>30) by Occupation | sort by Occupation;
Customers | summarize f_list = make_list_if(FirstName, Age>30, 1) by Occupation | sort by Occupation;
print '-- make_set() --';
Customers | summarize f_list = make_set(Education) by Occupation | sort by Occupation;
Customers | summarize f_list = make_set(Education, 2) by Occupation | sort by Occupation;
print '-- make_set_if() --';
Customers | summarize f_list = make_set_if(Education, Age>30) by Occupation | sort by Occupation;
Customers | summarize f_list = make_set_if(Education, Age>30, 1) by Occupation | sort by Occupation;
print '-- stdev() --';
Customers | project Age | summarize stdev(Age);
print '-- stdevif() --';
Customers | project Age | summarize stdevif(Age, Age%2==0);
print '-- binary_all_and --';
Customers | project Age | where Age > 40 | summarize binary_all_and(Age);
print '-- binary_all_or --';
Customers | project Age | where Age > 40 | summarize binary_all_or(Age);
print '-- binary_all_xor --';
Customers | project Age | where Age > 40 | summarize binary_all_xor(Age);

Customers | project Age | summarize percentile(Age, 95);
Customers | project Age | summarize percentiles(Age, 5, 50, 95)|project round(percentiles_Age[0],2),round(percentiles_Age[1],2),round(percentiles_Age[2],2);
Customers | project Age | summarize percentiles(Age, 5, 50, 95)[1];
Customers | summarize w=count() by AgeBucket=bin(Age, 5) | summarize percentilew(AgeBucket, w, 75);
Customers | summarize w=count() by AgeBucket=bin(Age, 5) | summarize percentilesw(AgeBucket, w, 50, 75, 99.9);

print '-- Summarize following sort --';
Customers | sort by FirstName | summarize count() by Occupation | sort by Occupation;

print '-- summarize with bin --';
EventLog | summarize count=count() by bin(Created, 1000);
EventLog | summarize count=count() by bin(unixtime_seconds_todatetime(Created/1000), 1s);
EventLog | summarize count=count() by time_label=bin(Created / 1000 * 1s, 1s);
Dates | project bin(EventTime, 1m);
print '-- make_list_with_nulls --';
Customers | summarize t = make_list_with_nulls(FirstName);
Customers | summarize f_list = make_list_with_nulls(FirstName) by Occupation;
Customers | summarize f_list = make_list_with_nulls(FirstName), a_list = make_list_with_nulls(Age) by Occupation;
print '-- count_distinct --';
Customers | summarize count_distinct(Education);
print '-- count_distinctif --';
Customers | summarize count_distinctif(Education, Age > 30);

print '-- format_datetime --';
EventLog | summarize count() by dt = format_datetime(bin(unixtime_seconds_todatetime(Created), 1d), 'yy-MM-dd') | order by dt asc;

print '-- take_any --';
Customers | summarize take_any(FirstName);
Customers | summarize take_any(FirstName), take_any(LastName);
Customers | where FirstName startswith 'C' | summarize take_any(FirstName, LastName) by FirstName, LastName;
Customers | summarize take_any(strcat(FirstName,LastName));
print '-- take_anyif --';
Customers | summarize take_anyif(FirstName, LastName has 'Diaz');
Customers | summarize take_anyif(FirstName, LastName has 'Diaz'), dcount(FirstName);

-- TODO:
-- arg_max()
-- arg_min()
