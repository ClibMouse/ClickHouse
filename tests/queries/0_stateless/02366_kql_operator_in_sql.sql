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

drop table if exists StormEventsLite;
create table StormEventsLite
(
    Id UUID materialized generateUUIDv4(),
    EventType String,
    index EventTypeIndex EventType TYPE tokenbf_v1(512, 3, 0) GRANULARITY 1,
    primary key(Id)
) engine = MergeTree;

insert into StormEventsLite select 'iddqd strong wind iddqd' from numbers(10000);
insert into StormEventsLite select 'Strong Wind' from numbers(10000);
insert into StormEventsLite select 'strong wind' from numbers(10000);
insert into StormEventsLite select 'iddqd Strong wind iddqd' from numbers(10000);
insert into StormEventsLite select 'iddqd Strong Wind iddqd' from numbers(10000);

-- explain indexes = 1 select count(*) from StormEventsLite where hasToken(EventType, 'strong');

select '-- #1 --' ;
select * from kql(Customers | where FirstName !in ('Peter', 'Latoya'));
select '-- #2 --' ;
select * from kql(Customers | where FirstName !in ("test", "test2"));
select '-- #3 --' ;
select * from kql(Customers | where FirstName !contains 'Pet');
select '-- #4 --' ;
select * from kql(Customers | where FirstName !contains_cs 'Pet');
select '-- #5 --' ;
select * from kql(Customers | where FirstName !endswith 'ter');
select '-- #6 --' ;
select * from kql(Customers | where FirstName !endswith_cs 'ter');
select '-- #7 --' ;
select * from kql(Customers | where FirstName != 'Peter');
select '-- #8 --' ;
select * from kql(Customers | where FirstName !has 'Peter');
select '-- #9 --' ;
select * from kql(Customers | where FirstName !has_cs 'peter');
select '-- #10 --' ;
select * from kql(Customers | where FirstName !hasprefix 'Peter');
select '-- #11 --' ;
select * from kql(Customers | where FirstName !hasprefix_cs 'Peter');
select '-- #12 --' ;
select * from kql(Customers | where FirstName !hassuffix 'Peter');
select '-- #13 --' ;
select * from kql(Customers | where FirstName !hassuffix_cs 'Peter');
select '-- #14 --' ;
select * from kql(Customers | where FirstName !startswith 'Peter');
select '-- #15 --' ;
select * from kql(Customers | where FirstName !startswith_cs 'Peter');
select '-- #16 --' ;
select * from kql(print t = 'a' in~ ('A', 'b', 'c'));
select '-- #17 --' ;
select * from kql(Customers | where FirstName in~ ('peter', 'apple'));
select '-- #18 --' ;
select * from kql(Customers | where FirstName in~ ((Customers | project FirstName | where FirstName == 'Peter')));
select '-- #19 --' ;
select * from kql(Customers | where FirstName in~ ((Customers | project FirstName | where Age < 30)));
select '-- #20 --' ;
select * from kql(print t = 'a' !in~ ('A', 'b', 'c'));
select '-- #21 --' ;
select * from kql(Customers | where FirstName !in~ ('peter', 'apple'));
select '-- #22 --' ;
select * from kql(Customers | where FirstName !in~ ((Customers | project FirstName | where FirstName == 'Peter')));
select '-- #23 --' ;
select * from kql(Customers | where FirstName !in~ ((Customers | project FirstName | where Age < 30)));
select '-- #24 --' ;
select * from kql(Customers | where FirstName =~ 'peter' and LastName =~ 'naRA');
select '-- #25 --' ;
select * from kql(Customers | where FirstName !~ 'nEyMaR' and LastName =~ 'naRA');
select '-- #26 --' ;
select * from kql(print x = tostring(1d + 1d) | where tostring(x) !~ "asd");
select '-- #27 --' ;
select * from kql(Customers | where FirstName !in~ ('nEyMaR', 'Peter') | count);
select '-- operator has, !has, has_cs, !has_cs, has_all, has_any --';
select * from kql(StormEventsLite | where EventType has 'strong' | count);
select * from kql(StormEventsLite | where EventType !has 'strong wind' | count);
select * from kql(StormEventsLite | where EventType has_cs 'Strong Wind' | count);
select * from kql(StormEventsLite | where EventType !has_cs 'iddqd' | count);
select * from kql(StormEventsLite | where EventType has_all ('iddqd', 'string') | count);
select * from kql(StormEventsLite | where EventType has_any ('iddqd', 'string') | count);
DROP TABLE IF EXISTS Customers;
drop table if exists StormEventsLite;
