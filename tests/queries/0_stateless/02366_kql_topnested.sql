DROP TABLE IF EXISTS sales;
CREATE TABLE sales
(salesdate String,salesperson String,region String,amount UInt32) ENGINE = Memory;

INSERT INTO sales VALUES ( '12/31/1995','Robert','ON',1);
INSERT INTO sales VALUES ( '12/31/1995','Joseph','ON',2);
INSERT INTO sales VALUES ( '12/31/1995','Joseph','QC',3);
INSERT INTO sales VALUES ( '12/31/1995','Joseph','MA',4);
INSERT INTO sales VALUES ( '12/31/1995','Steven','QC',5);
INSERT INTO sales VALUES ( '03/29/1996','Joseph','ON',6);
INSERT INTO sales VALUES ( '03/29/1996','Robert','QC',7);
INSERT INTO sales VALUES ( '03/29/1996','Joseph','ON',8);
INSERT INTO sales VALUES ( '03/29/1996','Joseph','BC',9);
INSERT INTO sales VALUES ( '03/29/1996','Joseph','QC',10);
INSERT INTO sales VALUES ( '03/29/1996','Joseph','MA',11);
INSERT INTO sales VALUES ( '03/29/1996','Steven','ON',12);
INSERT INTO sales VALUES ( '03/29/1996','Steven','QC',13);
INSERT INTO sales VALUES ( '03/29/1996','Steven','MA',14);
INSERT INTO sales VALUES ( '03/30/1996','Robert','ON',15);
INSERT INTO sales VALUES ( '03/30/1996','Robert','QC',16);
INSERT INTO sales VALUES ( '03/30/1996','Robert','MA',17);
INSERT INTO sales VALUES ( '03/30/1996','Joseph','ON',18);
INSERT INTO sales VALUES ( '03/30/1996','Joseph','BC',19);
INSERT INTO sales VALUES ( '03/30/1996','Joseph','QC',20);
INSERT INTO sales VALUES ( '03/30/1996','Joseph','MA',21);
INSERT INTO sales VALUES ( '03/30/1996','Steven','ON',22);
INSERT INTO sales VALUES ( '03/30/1996','Steven','QC',23);
INSERT INTO sales VALUES ( '03/30/1996','Steven','MA',24);
INSERT INTO sales VALUES ( '03/31/1996','Robert','MA',25);
INSERT INTO sales VALUES ( '03/31/1996','Thomas','ON',26);
INSERT INTO sales VALUES ( '03/31/1996','Thomas','BC',27);
INSERT INTO sales VALUES ( '03/31/1996','Thomas','QC',28);
INSERT INTO sales VALUES ( '03/31/1996','Thomas','MA',29);
INSERT INTO sales VALUES ( '03/31/1996','Steven','ON',30);


set dialect = 'kusto';

print '-- top nested 1 layer--';
sales | top-nested 3 of region by sum(amount)|order by region;

print '--top nested 2 layers--';
sales | top-nested 3 of region by sum(amount), top-nested 2 of salesperson by sum(amount)|order by region, salesperson;

print '--top nested 3 layers--';
sales | top-nested 3 of region by sum(amount), top-nested 2 of salesperson by sum(amount), top-nested 2 of salesdate by sum(amount)|order by region, salesperson, salesdate;

print '--top nested 1 layer with others--';
sales | top-nested 3 of region with others = 'all other region' by sum(amount)|order by region;

print '--top nested 2 layers with 2 others--';
sales | top-nested 3 of region with others = 'all other region' by sum(amount),  top-nested 2 of salesperson with others = 'all other person' by sum(amount)|order by region, salesperson;

print '--top nested 2 layers with 1st others--';
sales | top-nested 3 of region with others = 'all other region' by sum(amount),  top-nested 2 of salesperson by sum(amount)|order by region, salesperson;

print '--top nested 2 layer with 2nd others--';
sales | top-nested 3 of region  by sum(amount), top-nested 2 of salesperson with others = 'all other person' by sum(amount)|order by region, salesperson;

print '--top nested 3 layers with 3 others--';
sales | top-nested 3 of region with others = 'all other region' by sum(amount),  top-nested 2 of salesperson with others = 'all other person' by sum(amount), top-nested 2 of salesdate with others = 'all other date' by sum(amount)|order by region, salesperson, salesdate;

print '--top nested use expression as aggregation--';
sales | top-nested 3 of region by sum(amount)*2 + 5|order by region;

print '--top nested use expression as top n--';
sales | top-nested strlen('abc') of region by sum(amount)|order by region;

print '--top nested use expression as others--';
sales | top-nested 3 of region with others = strcat("all other"," region") by sum(amount)|order by region;

print '--top nested use expression as column--';
sales | top-nested of substring(region,0,1) by sum(amount)|order by Column1;

print '--top nested without top n--';
sales | top-nested of region by sum(amount)|order by region;