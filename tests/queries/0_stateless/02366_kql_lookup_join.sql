DROP TABLE IF EXISTS FactTable;
CREATE TABLE FactTable (Row String, Personal String, Family String) ENGINE = Memory;
INSERT INTO FactTable VALUES  ('1', 'Bill',   'Gates');
INSERT INTO FactTable VALUES  ('2', 'Bill',   'Clinton');
INSERT INTO FactTable VALUES  ('3', 'Bill',   'Clinton');
INSERT INTO FactTable VALUES  ('4', 'Steve',  'Ballmer');
INSERT INTO FactTable VALUES  ('5', 'Tim',    'Cook');

DROP TABLE IF EXISTS DimTable;
CREATE TABLE DimTable (Personal String, Family String, Alias String) ENGINE = Memory;
INSERT INTO DimTable VALUES  ('Bill',  'Gates',   'billg');
INSERT INTO DimTable VALUES  ('Bill',  'Clinton', 'billc');
INSERT INTO DimTable VALUES  ('Steve', 'Ballmer', 'steveb');
INSERT INTO DimTable VALUES  ('Tim',   'Cook',    'timc');

DROP TABLE IF EXISTS X;
CREATE TABLE X (Key String, Value1 Int64) ENGINE = Memory;
INSERT INTO X VALUES  ('a',1);
INSERT INTO X VALUES  ('b',2);
INSERT INTO X VALUES  ('b',3);
INSERT INTO X VALUES  ('c',4);

DROP TABLE IF EXISTS Y;
CREATE TABLE Y  (Key String, Value2 Int64) ENGINE = Memory;
INSERT INTO  Y  VALUES  ('b',10);
INSERT INTO  Y  VALUES  ('c',20);
INSERT INTO  Y  VALUES  ('c',30);
INSERT INTO  Y  VALUES  ('d',40);

set dialect='kusto';

print '-- lookup 1 --';
FactTable | lookup kind=leftouter DimTable on Personal, Family | order by Row asc;
print '-- lookup 2 --';
FactTable | lookup kind=inner  DimTable on Personal, Family | order by Row asc;
print '-- lookup 3 --';
FactTable | lookup kind=leftouter (DimTable | where Personal == 'Bill') on Personal, Family | order by Row asc;
print '-- lookup 4 --';
FactTable | project Row, Personal , Family| lookup kind=leftouter DimTable on Personal, Family | order by Row asc;
print '-- lookup 5 --';
FactTable |project Row, Personal , Family| lookup kind=leftouter (DimTable | where Personal == 'Bill') on Personal, Family| lookup kind=inner DimTable on Personal, Family | order by Row asc;

print '-- Default join --';
X | order by Key, Value1 | join ( Y |  order by Key, Value2 )  on $left.Key == $right.Key | order by Key, Value1, Value2;
print '-- Default join 2--';
X | order by Key, Value1 | join kind=innerunique ( Y |  order by Key, Value2 )  on Key | order by Key, Value1, Value2;
print '-- Inner-join --';
X | order by Key, Value1 | join kind=inner ( Y |  order by Key, Value2 )  on Key | order by Key, Value1, Value2;
print '-- Left outer-join --';
X | order by Key, Value1 | join kind=leftouter ( Y |  order by Key, Value2 )  on Key | order by Key, Value1, Value2;
print '-- Right outer-join --';
X | order by Key, Value1 | join kind=rightouter ( Y |  order by Key, Value2 )  on Key | order by Key, Value1, Value2;
print '-- Full outer-join  --';
X | order by Key, Value1 | join kind=fullouter ( Y |  order by Key, Value2 )  on Key | order by Key, Value1, Value2;
print '-- Left anti-join --';
X | order by Key, Value1 | join kind=leftanti ( Y |  order by Key, Value2 )  on Key | order by Key, Value1, Value2;
print '-- Right anti-join --';
X | order by Key, Value1 | join kind=rightanti ( Y |  order by Key, Value2 )  on Key | order by Key, Value1, Value2;
print '-- Left semi-join --';
X | order by Key, Value1 | join kind=leftsemi ( Y |  order by Key, Value2 )  on Key | order by Key, Value1, Value2;
print '-- Right semi-join --';
X | order by Key, Value1 | join kind=rightsemi ( Y |  order by Key, Value2 )  on Key | order by Key, Value1, Value2;