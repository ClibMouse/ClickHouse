## KQL implemented features  
# April XX, 2023
## Bugfixes
 - Corrected an issue with parse_url in which hostnames and port numbers were not correctly parsed.
   ```
   parse_url follows the folowing structure.

   Scheme://Username:Password@Host:Port/Path?QueryParameters#Fragment

   '://' is required for further parsing.
   All other fields are optional and are parsed from left to right.
   Username and Password are parsed together, require ':' and '@', and will not match if either contains '/', '?', or '#'.
   IPv6 addresses are required to be encapsulated in brackets. 
   Host ends with '/', ':', '?' or '#'.
   Port starts with ':' and ends with '/', '?' or '#'.
   Path requires to start with '/' and ends with '?' or '#'. 
   Query Parameters is recursive, starts with '?', ends with '#', expected to be in the form of argument=value, and separated by '&'. 
   Fragment must start with '#'.

   Notes on differences between ADX and ClickHouse:

   ClickHouse will return a formated string. 'extract_json' can be used to convert the string.
   print x = parse_url("http://[2001:0db8:0000:0000:0000:ff00:0042:8329]?asd=qwe&qwe=asd") | project extract_json("$.Scheme", x); 
   ClickHouse includes Path as '/' where ADX requires anything after '/' to populate Path.
   print parse_url("http://host:1234/");
   ClickHouse includes Port where ADX requires '/' for Port and without '/' will treat Port as part of Host.
   print parse_url("http://host:1234?arg=value")
   ClickHouse includes arg value in Query parameters where ADX treats this as host.
   print parse_url("http://?arg=value");
   ClickHouse will not parse IPv6 addresses not encapsulated in brackets [RFC 3986](https://datatracker.ietf.org/doc/html/rfc3986#section-3.2.2) 
   Correct IPv6 
   print parse_url("http://[2001:db8:3333:4444:5555:6666:7777:8888]:1234/filepath/index.htm")
   Incorrect IPv6
   print parse_url("http://2001:db8:3333:4444:5555:6666:7777:8888:1234/filepath/index.htm");
   print parse_url("http://2001:db8:3333:4444:5555:6666:7777:8888/filepath/index.htm");
   ADX will incorrectly consume part of encapsulated IPv6 Host as Port from last colon to '/'.
   print parse_url("http://[2001:db8:3333:4444:5555:6666:7777:8888]/filepath/index.htm")
   ```
## Functions
- [indexof_regex](https://learn.microsoft.com/en-us/azure/data-explorer/kusto/query/indexofregexfunction)
   `print idx1 = indexof_regex("abcabc", "a.c");`
- [make_string()](https://github.com/microsoft/Kusto-Query-Language/blob/master/doc/makestringfunction.md)
   `print str = make_string(75, 117, 115, 116, 111)`

# March XX, 2023
## Functions
- [hash()](https://learn.microsoft.com/en-us/azure/data-explorer/kusto/query/hashfunction)
   `print hash('World')`
   Note: ClickHouse will attempt to fit a number within the smallest data type possible. As a result
   Int32 data types not cast with KQL int() may not match ADX results.
- [hash_sha256()](https://learn.microsoft.com/en-us/azure/data-explorer/kusto/query/sha256hashfunction)
   `print hash_sha256('World')`
- [not()](https://learn.microsoft.com/en-us/azure/data-explorer/kusto/query/notfunction)
   `print not(1)`
## Special Functions
- [toscalar](https://learn.microsoft.com/en-us/azure/data-explorer/kusto/query/toscalarfunction)
   `range z from toscalar(print x=1) to toscalar(range x from 1 to 9 step 1 | count) step toscalar(2);`

## Operator
 - [between, !between](https://learn.microsoft.com/en-us/azure/data-explorer/kusto/query/betweenoperator)
    ```
    select * from kql(TableWithVariousDataTypes | project Age | where Age between (10 .. 12));
    select * from kql(TableWithVariousDataTypes | project Age | where Age !between (10 .. 30));
    select * from kql(TableWithVariousDataTypes | project Height | where Height between (5.2 .. 6.6));
    select * from kql(TableWithVariousDataTypes | project Height | where Height !between (5.3 .. 7.6));
    select * from kql(TableWithVariousDataTypes | project JoinDate | where JoinDate between (datetime('2020-01-01') .2d));
    select * from kql(TableWithVariousDataTypes | project JoinDate | where JoinDate !between (datetime('2020-01-01') .. 2d));
    select * from kql(TableWithVariousDataTypes | project JoinDate | where JoinDate between (datetime('2020-06-30') .. datetime('2025-06-30')));
    select * from kql(TableWithVariousDataTypes | project JoinDate | where JoinDate !between (datetime('2020-06-30') .. datetime('2025-06-30')));
    ```
# March 15, 2023
## Feature
 - KQL - improve timespan textual representation in the CLI
   The textual representation of `timespan` will now be identical to ADX, whenever the `dialect` setting is `kusto` or `kusto_auto`. The internal representation shall remain unchanged as `IntervalNanosecond`. In essence, any `Interval` type will also be represented this way even when running regular SQL queries as long as the `dialect` option is `kusto_auto`.
   `print a = timespan(2d), b = timespan(4h), c = timespan(8m), d = timespan(16s), e = timespan(123millis), f = timespan(456micros), g = timespan(789nanos) | extend x = a + b + c + d + e + f + g;`
## Operator
- [getschema](https://learn.microsoft.com/en-us/azure/data-explorer/kusto/query/getschemaoperator)
   `print x = 'asd' | extend strlen(x) | getschema`

## Bugfixes
## Functions
- [has_ipv4()](https://learn.microsoft.com/en-us/azure/data-explorer/kusto/query/has-ipv4-function)
   `print has_ipv4('10:00:00 192.168.1.1 GET /index.html 404', '192.168.1.1')`
- [has_any_ipv4()](https://learn.microsoft.com/en-us/azure/data-explorer/kusto/query/has-any-ipv4-function)
   `print has_any_ipv4('10:00:00 192.168.1.1 GET /index.html 404', '127.0.0.1', '192.168.1.1')`
- [has_ipv4_prefix()](https://learn.microsoft.com/en-us/azure/data-explorer/kusto/query/has-ipv4-prefix-function)
   `print has_ipv4_prefix('10:00:00 192.168.1.1 GET /index.html 404', '192.168.')`
- [has_any_ipv4_prefix()](https://learn.microsoft.com/en-us/azure/data-explorer/kusto/query/has-any-ipv4-prefix-function)
   `print has_any_ipv4_prefix('10:00:00 192.168.1.1 GET /index.html 404', '127.', '192.168.1.')`
- [string_size()](https://github.com/microsoft/Kusto-Query-Language/blob/master/doc/stringsizefunction.md)
   `print string_size('⒦⒰⒮⒯⒪')`
- [to_utf8()](https://github.com/microsoft/Kusto-Query-Language/blob/master/doc/toutf8function.md)
   `print arr = to_utf8('⒦⒰⒮⒯⒪')`
- [new_guid()](https://github.com/microsoft/Kusto-Query-Language/blob/master/doc/newguidfunction.md)
   `print g = new_guid()`
- [gettype()](https://learn.microsoft.com/en-us/azure/data-explorer/kusto/query/gettypefunction)
   `print t = gettype(1)`
- has_ipv6()
  `Note: Not part of Microsoft's Kusto. Functions similar to has_ipv4()`
  `print has_ipv6('09:46:00 2600:1404:6400:1695::1e89 GET /favicon.ico 404', '2600:1404:6400:1695::1e89')`
- has_any_ipv6()
  ```
  Note: Not part of Microsoft's Kusto. Functions similar to has_any_ipv4()
  print has_any_ipv6('09:46:00 2600:1404:6400:1695::1e89', '::1', '2600:1404:6400:1695::1e89')
  ```
- has_ipv6_prefix()
  ```
  Note: Not part of Microsoft's Kusto. Functions similar to has_ipv4_prefix() although the prefixes can't contain :: compression syntax.
  print has_ipv6_prefix('09:46:00 2600:1404:6400:1695::1e89', '2600:1404:6400:1695:0:0:0:')
  ```
- has_any_ipv6_prefix()
  ```
  Note: Not part of Microsoft's Kusto. Functions similar to has_any_ipv4_prefix() although the prefixes can't contain :: compression syntax.
  print has_any_ipv6_prefix('09:46:00 2600:1404:6400:1695::1e89', '0:0:0:0:0:ffff:127.', '2600:1404:6400:1695:')
  ```

# February 28, 2023
## Operator
- [project-away](https://learn.microsoft.com/en-us/azure/data-explorer/kusto/query/projectawayoperator)
    ```
    print '1-- remove one column';
    Customers | project-away FirstName;
    print '2-- remove two columns';
    Customers | project-away FirstName, LastName;
    print '3-- remove columns by one wildcard';
    Customers | project-away *Name;
    print '4-- remove columns by two wildcards';
    Customers | project-away *Name, *tion;
    print '5-- remove columns by one wildcard, one regular column';
    Customers | project-away *Name, Age;
    print '6-- remove columns by one wildcard, two regular column';
    Customers | project-away *Name, Age, Education;
    print '7-- remove columns by two wildcard, two regular column';
    Customers | project-away *irstName, Age, *astName, Education;
    print '8-- remove one column from previous piple result';
    Customers | where Age< 30 | limit 2 | project-away FirstName;
    print '9-- remove one column from summized piple result';
    Customers|summarize sum(Age), avg(Age) by FirstName | project-away sum_Age;
    print '10-- remove columns after extend';
    Customers|extend FullName = strcat(FirstName,' ',LastName) | project-away FirstName, LastName;  
    ```


## Bugfixes
- Fixed count operator issue (2112):
    ```
    Customers|project FirstName|where FirstName != 'Peter'|sort by FirstName asc nulls first|count
    ```
- Fixed KQL sub-query issues:  
    - Multiple columns in sub-query.  
      Multiple columns in sub-query works in KQL ADX but only the first column is effective, while not working in ClickHouse. this fixed issue. e.g.
      ```
      Customers | where FirstName in ((Customers|project FirstName, LastName))
      ```
      limitation: the  `select *` noit work in sub-querym because there's individula column.  
    - Negative operators in sub-query  
      fixed the issue for negative operators not work in KQL sub-query. e.g
      ```
      Customers | where FirstName in ((Customers|project FirstName, LastName|where FirstName !has 'Peter'))
      ``` 
    - Case-insensitive compare in sub-query
      fixed the case-insensitive compare issuse for multiple pipe in sub-query. e.g
      ```
      Customers | where FirstName in~ ((Customers|where FirstName !has 'Peter'|project FirstName, LastName))
      ```
    - Check functional test `tests/queries/0_stateless/02366_kql_test_subquery.sql` for details.

- KQL - has operator fails to return result when needle has separator character
- strcat_delim fails when encountered with escaped double quotes (2159)
- summarize throw exception if Aggregation is missing (2113)
- todecimal() doesn't work with column arguments (1413)
- extract_json value cast to boolean causes exception (1490)
- [isempty() and isnotempty() not accepting non-quoted strings]

## Functions
- [abs()](https://learn.microsoft.com/en-us/azure/data-explorer/kusto/query/abs-function)
   `print abs(-5)`
- [acos()](https://learn.microsoft.com/en-us/azure/data-explorer/kusto/query/acosfunction)
- [asin()](https://learn.microsoft.com/en-us/azure/data-explorer/kusto/query/asinfunction)
   `print asin(0.5)`
- [atan()](https://learn.microsoft.com/en-us/azure/data-explorer/kusto/query/atanfunction)
   `print atan(0.5)`
- [atan2()](https://learn.microsoft.com/en-us/azure/data-explorer/kusto/query/atan2function)
   `print atan2(1,1)`
- [ceiling()](https://learn.microsoft.com/en-us/azure/data-explorer/kusto/query/ceilingfunction)
   `print c1 = ceiling(-1.1), c2 = ceiling(0), c3 = ceiling(0.9)`
- [cos()](https://learn.microsoft.com/en-us/azure/data-explorer/kusto/query/cosfunction)
   `print cos(1)`
- [cot()](https://learn.microsoft.com/en-us/azure/data-explorer/kusto/query/cotfunction)
   `print cot(1)`
- [degrees()](https://learn.microsoft.com/en-us/azure/data-explorer/kusto/query/degreesfunction)
   `print degrees(pi()/4)`
- [exp()](https://learn.microsoft.com/en-us/azure/data-explorer/kusto/query/exp-function)
   `print exp(2)`
- [exp2()](https://learn.microsoft.com/en-us/azure/data-explorer/kusto/query/exp2-function)
   `print exp2(2)`
- [exp10()](https://learn.microsoft.com/en-us/azure/data-explorer/kusto/query/exp10-function)
   `print exp10(3)`
- [gamma()](https://learn.microsoft.com/en-us/azure/data-explorer/kusto/query/gammafunction)
- [isfinite()](https://learn.microsoft.com/en-us/azure/data-explorer/kusto/query/isfinitefunction)
   `print isfinite(1.0/0.0)`
- [isinf()](https://learn.microsoft.com/en-us/azure/data-explorer/kusto/query/isinffunction)
   `print isinf(1.0/0.0)`
- [log()](https://learn.microsoft.com/en-us/azure/data-explorer/kusto/query/log-function)
   `print log(5)`
- [log2()](https://learn.microsoft.com/en-us/azure/data-explorer/kusto/query/log2-function)
   `print log2(5)`
- [log10()](https://learn.microsoft.com/en-us/azure/data-explorer/kusto/query/log10-function)
   `print log10(5)`
- [loggamma()](https://learn.microsoft.com/en-us/azure/data-explorer/kusto/query/loggammafunction)
   `print loggamma(5)`
- [max_of()](https://learn.microsoft.com/en-us/azure/data-explorer/kusto/query/max-offunction)
   `print result = max_of(10, 1, -3, 17)` 
- [min_of()](https://learn.microsoft.com/en-us/azure/data-explorer/kusto/query/min-offunction)
   `print result = min_of(10, 1, -3, 17)` 
- [pi()](https://learn.microsoft.com/en-us/azure/data-explorer/kusto/query/pifunction)
   `print pi()`
- [pow()](https://learn.microsoft.com/en-us/azure/data-explorer/kusto/query/powfunction)
   `print pow(2, 3)`
- [radians()](https://learn.microsoft.com/en-us/azure/data-explorer/kusto/query/radiansfunction)
   `print radians0 = radians(90), radians1 = radians(180), radians2 = radians(360)` 
- [rand()](https://learn.microsoft.com/en-us/azure/data-explorer/kusto/query/randfunction)
   `print rand(1000)`
- [round()](https://learn.microsoft.com/en-us/azure/data-explorer/kusto/query/roundfunction)
   `print round(2.15, 1)`
- [sign()](https://learn.microsoft.com/en-us/azure/data-explorer/kusto/query/signfunction)
   `print s1 = sign(-42), s2 = sign(0), s3 = sign(11.2)`
- [sin()](https://learn.microsoft.com/en-us/azure/data-explorer/kusto/query/sinfunction)
- [sqrt()](https://learn.microsoft.com/en-us/azure/data-explorer/kusto/query/sqrtfunction)
   `print sqrt(256)`
- [tan()](https://learn.microsoft.com/en-us/azure/data-explorer/kusto/query/tanfunction)
- [variance()](https://learn.microsoft.com/en-us/azure/data-explorer/kusto/query/variance-aggfunction)
   `Customers | summarize variance(Age);`
- [variancep()](https://learn.microsoft.com/en-us/azure/data-explorer/kusto/query/variancep-aggfunction)
   `Customers | summarize variancep(Age);`
- [varianceif()](https://learn.microsoft.com/en-us/azure/data-explorer/kusto/query/varianceif-aggfunction)
   `Customers | summarize varianceif(Age, Age < 30)`
- [lookup()](Not a KQL function, it's an IBM specific suggested implementation.
Supports simple keys only. Do not suppoer RANGE_HASHED keys.)
   `print lookup('dictionary_table', 'value', '1')`
   `print lookup('dictionary_table', 'value', '100', 'default')`

# January 26, 2023
## Functions
- [range()](https://learn.microsoft.com/en-us/azure/data-explorer/kusto/query/rangefunction)  
Difference from ADX:  
   Return  empty array [] if range is empty, while ADX return NULL
   The maxamum number of elements of array is 1000000 (limitation of clickhouse), 1,048,576  in ADX
```
print '-- range function int, int, int --';
print range(1, 10, 2);
print '-- range function int, int --';
print range(1, 10);
print '-- range function float, float, float --';
print range(1.2, 10.3, 2.2);
print '-- range function positive float, float, int --';
print range(1.2, 10.3, 2);
print '-- range function positive float, int, float --';
print range(1.2, 10, 2.2);
print '-- range function positive integer, int, float --';
print range(1, 10, 2.2);
print '-- range function positive integer, float, float --';
print range(1, 10.5, 2.2);
print '-- range function positive float, int, int --';
print range(1.2, 10, 2);
print '-- range function positive int, int, negative int --';
print range(12, 3, -2);
print '-- range function positive float, int, negative float --';
print range(12.8, 3, -2.3);
print '-- range function datetime, datetime, timespan --';
print range(datetime('2001-01-01'), datetime('2001-01-02'), 5h);
print '-- range function datetime, datetime, negative timespan --';
print range(datetime('2001-01-03'), datetime('2001-01-02'), -5h);
print '-- range function datetime, datetime --';
print range(datetime('2001-01-01'), datetime('2001-01-02'));
print '-- range function timespan, timespan, timespan --';
print range(1h, 5h, 2h);
print '-- range function timespan, timespan --';
print range(1h, 5h);
print '-- range function timespan, timespan, negative timespan --';
print range(11h, 5h, -2h);
print '-- range function float timespan, timespan, timespan --';
print range(1.5h, 5h, 2h);
print '-- range function endofday, endofday, timespan --';
print range(endofday(datetime(2017-01-01 10:10:17)), endofday(datetime(2017-01-03 10:10:17)), 1d);
```  

## Improvement
- [dcount()](https://learn.microsoft.com/en-us/azure/data-explorer/kusto/query/dcount-aggfunction) and [dcountif()](https://learn.microsoft.com/en-us/azure/data-explorer/kusto/query/dcountif-aggfunction)
   docunt and dcountif now accept the additional accuracy parameter which is the base-2 logarithm of the number of cells in  [HyperLogLog](https://en.wikipedia.org/wiki/HyperLogLog).
## Case Insensitive Operators
- [in~](https://learn.microsoft.com/en-us/azure/data-explorer/kusto/query/inoperator)  
   `print t = 'a' in~ ('A', 'b', 'c')`  
   `Customers | where FirstName in~ ((Customers | project FirstName | where FirstName == 'Peter'))`  
- [!in~](https://learn.microsoft.com/en-us/azure/data-explorer/kusto/query/not-in-operator)  
   `print t = 'a' !in~ (dynamic(['A', 'b', 'c']))`  
   `Customers | where FirstName !in~ ('peter', 'apple')`  
- [=~](https://learn.microsoft.com/en-us/azure/data-explorer/kusto/query/equals-operator)  
   `Customers | where FirstName =~ 'peter' and LastName =~ 'naRA'`  
- [!~](https://learn.microsoft.com/en-us/azure/data-explorer/kusto/query/not-equals-operator)  
   `Customers | where FirstName !~ 'nEyMaR' and LastName =~ 'naRA'`  
## Aggregate Functions  
- [take_any()](https://learn.microsoft.com/en-us/azure/data-explorer/kusto/query/take-any-aggfunction)
   ```
   Note: * is not currently a supported argument.
   ```
- [take_anyif()](https://learn.microsoft.com/en-us/azure/data-explorer/kusto/query/take-anyif-aggfunction)
- [dcount() and dcountif()]
## Operator
- [range](https://learn.microsoft.com/en-us/azure/data-explorer/kusto/query/rangeoperator)  
   `range LastWeek from ago(7d) to now() step 1d`  
   `range Steps from 1 to 8 step 3`  
- [top-nested](https://learn.microsoft.com/en-us/azure/data-explorer/kusto/query/topnestedoperator)

   ```
   CREATE TABLE sales
   (salesdate String,salesperson String,region String,amount UInt32) ENGINE = Memory;

   INSERT INTO sales VALUES ( '12/31/1995','Robert','ON-Ontario',1);
   INSERT INTO sales VALUES ( '12/31/1995','Joseph','ON-Ontario',2);
   INSERT INTO sales VALUES ( '12/31/1995','Joseph','QC-Quebec',3);
   INSERT INTO sales VALUES ( '12/31/1995','Joseph','MA-Manitoba',4);
   INSERT INTO sales VALUES ( '12/31/1995','Steven','QC-Quebec',5);
   INSERT INTO sales VALUES ( '03/29/1996','Joseph','ON-Ontario',6);
   INSERT INTO sales VALUES ( '03/29/1996','Robert','QC-Quebec',7);
   INSERT INTO sales VALUES ( '03/29/1996','Joseph','ON-Ontario',8);
   INSERT INTO sales VALUES ( '03/29/1996','Joseph','BC-British Columbia',9);
   INSERT INTO sales VALUES ( '03/29/1996','Joseph','QC-Quebec',10);
   INSERT INTO sales VALUES ( '03/29/1996','Joseph','MA-Manitoba',11);
   INSERT INTO sales VALUES ( '03/29/1996','Steven','ON-Ontario',12);
   INSERT INTO sales VALUES ( '03/29/1996','Steven','QC-Quebec',13);
   INSERT INTO sales VALUES ( '03/29/1996','Steven','MA-Manitoba',14);
   INSERT INTO sales VALUES ( '03/30/1996','Robert','ON-Ontario',15);
   INSERT INTO sales VALUES ( '03/30/1996','Robert','QC-Quebec',16);
   INSERT INTO sales VALUES ( '03/30/1996','Robert','MA-Manitoba',17);
   INSERT INTO sales VALUES ( '03/30/1996','Joseph','ON-Ontario',18);
   INSERT INTO sales VALUES ( '03/30/1996','Joseph','BC-British Columbia',19);
   INSERT INTO sales VALUES ( '03/30/1996','Joseph','QC-Quebec',20);
   INSERT INTO sales VALUES ( '03/30/1996','Joseph','MA-Manitoba',21);
   INSERT INTO sales VALUES ( '03/30/1996','Steven','ON-Ontario',22);
   INSERT INTO sales VALUES ( '03/30/1996','Steven','QC-Quebec',23);
   INSERT INTO sales VALUES ( '03/30/1996','Steven','MA-Manitoba',24);
   INSERT INTO sales VALUES ( '03/31/1996','Robert','MA-Manitoba',25);
   INSERT INTO sales VALUES ( '03/31/1996','Thomas','ON-Ontario',26);
   INSERT INTO sales VALUES ( '03/31/1996','Thomas','BC-British Columbia',27);
   INSERT INTO sales VALUES ( '03/31/1996','Thomas','QC-Quebec',28);
   INSERT INTO sales VALUES ( '03/31/1996','Thomas','MA-Manitoba',29);
   INSERT INTO sales VALUES ( '03/31/1996','Steven','ON-Ontario',30);

   print '-- top 3 regions by sales--';
   sales | top-nested 3 of region by sum(amount);

   print '-- top 2 salespeople in each of these regions?--';
   sales | top-nested 3 of region by sum(amount), top-nested 2 of salesperson by sum(amount);

   print '--top 3 and other regions by sales--';
   sales | top-nested 3 of region with others = 'all other region' by sum(amount);

   print '--top 3 and other regions by sales and top 2 and  other salespeople in each of these regions--';
   sales | top-nested 3 of region with others = 'all other region' by sum(amount),  top-nested 2 of salesperson with others = 'all other person' by sum(amount);

   print '--top 3 and other regions by sales and top 2 salespeople in each of these regions--';
   sales | top-nested 3 of region with others = 'all other region' by sum(amount),  top-nested 2 of salesperson by sum(amount)

   print '--top 3 regions by sales and top 2 and  other salespeople in each of these regions--';
   sales | top-nested 3 of region  by sum(amount), top-nested 2 of salesperson with others = 'all other person' by sum(amount);

   print '--top 3 regions by difference between max sales and min sales--';
   sales | top-nested 3 of region by sum(amount) - min(amount);

   print '-- top 3 regions using abbreviations by sales--';
   sales | top-nested 3 of substring(region, 0, 2)  by sum(amount);

   print '-- all top regions by sales--';
   sales | top-nested of region by sum(amount);
   ```

## Bugs
- [KQL Phase 2 - base64_encode_fromguid encodes strings as opposed to binary]  
- [KQL Phase 2: summarize with bin and format_datetime]  
- [make_datetime creates wrong date time]  
- [KQL Phase 2: summarize using bin has different result than Azure Data Explorer using the same sample data]  
- [KQL Phase 3: datetime should be rounded in certain cases]  
- [kql_bin does not accept DateTime type]  
- [KQL Phase 2 - totimespan should return null when conversion fails.]  
- [reverse() with datetime and timespan arguments needs to be improved.]  
- [String operator has throws exception when needle has white space or separator characters]  


# December 7, 2022

## Functions
- [count_distinct](https://learn.microsoft.com/en-us/azure/data-explorer/kusto/query/count-distinct-aggfunction)  
   `Customers | summarize count_distinct(Education);`  
- [count_distinctif](https://learn.microsoft.com/en-us/azure/data-explorer/kusto/query/count-distinctif-aggfunction)  
   `Customers | summarize count_distinctif(Education, Age > 30);`  
- [iff](https://learn.microsoft.com/en-us/azure/data-explorer/kusto/query/ifffunction)  
   `Customers | extend t = iff(Age <= 10, "smaller", "bigger");`  
- [iif](https://learn.microsoft.com/en-us/azure/data-explorer/kusto/query/iiffunction)  
   `Customers | extend t = iif(Age <= 10, "smaller", "bigger");`
## bug fixed
- [indexOf function doesn't work for extended parameters]  
- [Create generic function for time arithmetic]  
- [KQL Phase 2: tolong should return the number of ticks when supplied with a timespan]  

# November 23, 2022

## Operator
- [join](https://learn.microsoft.com/en-us/azure/data-explorer/kusto/query/joinoperator?pivots=azuredataexplorer)  
   ```
   CREATE TABLE X (Key String, Value1 Int64) ENGINE = Memory;
   INSERT INTO X VALUES  ('a',1);
   INSERT INTO X VALUES  ('b',2);
   INSERT INTO X VALUES  ('b',3);
   INSERT INTO X VALUES  ('c',4);
   
   CREATE TABLE Y  (Key String, Value2 Int64) ENGINE = Memory;
   INSERT INTO  Y  VALUES  ('b',10);
   INSERT INTO  Y  VALUES  ('c',20);
   INSERT INTO  Y  VALUES  ('c',30);
   INSERT INTO  Y  VALUES  ('d',40);

   Join flavor : 
  
   Default join is innerunique
      X | join Y  on $left.Key == $right.Key ;
      X | join kind=innerunique Y  on Key ;

   Inner-join
      X | join kind=inner Y  on Key ;

   Left outer-join
      X | join kind=leftouter Y  on Key ;

   Right outer-join
      X | join kind=rightouter Y  on Key ;

   Full outer-join 
      X | join kind=fullouter Y  on Key ;

   Left anti-join
      X | join kind=leftanti Y  on Key ;

   Right anti-join
      X | join kind=rightanti Y  on Key ;

   Left semi-join
      X | join kind=leftsemi Y  on Key ;

   Right semi-join
      X | join kind=rightsemi Y  on Key ;
   ```
   **Deviation from ADX**
   Because of the limitation between KQL and SQL. the result may different from ADX.(KQL-CH take the result of ClickHouse)
   - columns  
      ADX : common columns are duplicatedc in output
      KQL-CH : only one column for common columns 
   - column name  
      ADX : column with same name (not common) ->column1
      KQL-CH : column with same name (not common) -> right_.column
   - filters  
      ADX: Kusto is optimized to push filters that come after the join, towards the appropriate join side, left or right, when possible  
      KQL-CH: because in the domanin of KQL, does not know the schema of tables, so the push need to manually done by user, like: 
      ```
      t1|join kind = innerunique t2 on key | where value == 'val1.2'
      ```
      need to chang as the fowllowing by user(if user want) :
      ```
      t1| where value == 'val1.2' | join kind = innerunique t2 on key 
      ```
   - semi join flavor  
      ADX : only returns left side or right side columns  
      KQL-CH : returns columns from both side  
   - Join hints : not supported yet
- [lookup](https://learn.microsoft.com/en-us/azure/data-explorer/kusto/query/lookupoperator)  
   lookup is a subset of join, only support : kind=leftouter and kind=inner. if kind unspecified, kind=leftouter  
   ```
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

   FactTable | lookup kind=leftouter DimTable on Personal, Family 

   FactTable | lookup kind=inner  DimTable on Personal, Family 
   ```

## Bugs fixed
   - [Incorrect Regx conversion]  
   - [KQL phase 2 - timespan calculation results in exception]  
   - [KQL phase 2 - format_timespan returns incorrect results]  
   - [Bin function should support time intervals less than 1 second]  
   - [KQL Phase 2: datetime subtraction results in exception]  
   - [Timespan() doesn't parse bareword arguments.]
   - [KQL-phase2 distinct operator does not support alias]  

# November 7, 2022
## Improvement
- [array_sort_asc](https://learn.microsoft.com/en-us/azure/data-explorer/kusto/query/arraysortascfunction) and [array_sort_desc](https://learn.microsoft.com/en-us/azure/data-explorer/kusto/query/arraysortdescfunction)
   ```
   Returns the same number of arrays as in the input, with the first array sorted in ascending order, and the remaining arrays ordered to match the reordered first array.

   null will be returned for every array that differs in length from the first one.
   ```
   Because array in ClickHouse is not nullable, so an array with a single NULL  ( `[NULL]`) is returned instead of a null if array that differs in length from the first one:
   ```
   array_sort_asc(dynamic([2, 1, 3]), dynamic([20, 40, 30]), dynamic([100, 200])) -> [1,2,3,NULL],[10,20,30,40],[NULL]
   ```
   the result can be used as a condition 
   ```
   DROP TABLE IF EXISTS visit;
   CREATE TABLE visit(pageid UInt8, ip_country Array(Nullable(String)), hit Array(Int64),duration Array(Int64)) ENGINE = Memory;
   INSERT INTO visit VALUES (1,['CA', 'US','FR','Eng'], [11,16,12,20],[100,500,300,200]);
   INSERT INTO visit VALUES (2,['Japan', 'Gem','FR','Eng'], [31,22,33,10],[510,410,310,210]);
   INSERT INTO visit VALUES (3,['CA', 'Gem','Japan','Eng'], [25,10,23,11],[120,110,130]);
   INSERT INTO visit VALUES (4,['CA', 'Gem',null,'Eng'], [5,10,3,2],[220,320,310,150]);
   INSERT INTO visit VALUES (5,['FR', null,'US','Eng'], [16,12,23,10],[210,250,110,260]);

   visit | project *, array_sort_asc(ip_country, hit, duration)
   ┌─pageid─┬─ip_country─────────────────┬─hit───────────┬─duration──────────┬─kql_array_sort_asc(ip_country, hit, duration)────────────────┐
   │      2 │ ['Japan','Gem','FR','Eng'] │ [31,22,33,10] │ [510,410,310,210] │ (['Eng','FR','Gem','Japan'],[10,33,22,31],[210,310,410,510]) │
   └────────┴────────────────────────────┴───────────────┴───────────────────┴──────────────────────────────────────────────────────────────┘
   ┌─pageid─┬─ip_country─────────────┬─hit───────────┬─duration──────────┬─kql_array_sort_asc(ip_country, hit, duration)────────────┐
   │      1 │ ['CA','US','FR','Eng'] │ [11,16,12,20] │ [100,500,300,200] │ (['CA','Eng','FR','US'],[11,20,12,16],[100,200,300,500]) │
   └────────┴────────────────────────┴───────────────┴───────────────────┴──────────────────────────────────────────────────────────┘
   ┌─pageid─┬─ip_country──────────────┬─hit────────┬─duration──────────┬─kql_array_sort_asc(ip_country, hit, duration)──────────┐
   │      4 │ ['CA','Gem',NULL,'Eng'] │ [5,10,3,2] │ [220,320,310,150] │ (['CA','Eng','Gem',NULL],[5,2,10,3],[220,150,320,310]) │
   └────────┴─────────────────────────┴────────────┴───────────────────┴────────────────────────────────────────────────────────┘
   ┌─pageid─┬─ip_country─────────────────┬─hit───────────┬─duration──────┬─kql_array_sort_asc(ip_country, hit, duration)─────┐
   │      3 │ ['CA','Gem','Japan','Eng'] │ [25,10,23,11] │ [120,110,130] │ (['CA','Eng','Gem','Japan'],[25,11,10,23],[NULL]) │
   └────────┴────────────────────────────┴───────────────┴───────────────┴───────────────────────────────────────────────────┘
   ┌─pageid─┬─ip_country─────────────┬─hit───────────┬─duration──────────┬─kql_array_sort_asc(ip_country, hit, duration)────────────┐
   │      5 │ ['FR',NULL,'US','Eng'] │ [16,12,23,10] │ [210,250,110,260] │ (['Eng','FR','US',NULL],[10,16,23,12],[260,210,110,250]) │
   └────────┴────────────────────────┴───────────────┴───────────────────┴──────────────────────────────────────────────────────────┘  
   
   visit | where isnull((array_sort_asc(ip_country, hit, duration))[2][0])
   ┌─pageid─┬─ip_country─────────────────┬─hit───────────┬─duration──────┐
   │      3 │ ['CA','Gem','Japan','Eng'] │ [25,10,23,11] │ [120,110,130] │
   └────────┴────────────────────────────┴───────────────┴───────────────┘
   ```

   the following behaviours are same as Azure Data Explorer  
   if no alias specified, the functions return a single tuple includes arrays. can use array sbscripon to access the element inside. for exapmple:
   ```
   print array_sort_asc(dynamic([2, 1, 3]), dynamic([20, 40, 30]), dynamic([100, 200]))[0] -> [1,2,3]
   ```
   if a single alias is used the first array as an column is returned :
   ```
   print t = array_sort_asc(dynamic([2, 1, 3]), dynamic([20, 40, 30]), dynamic([100, 200]))
   ┌─t───────┐
   │ [1,2,3] │
   └─────────┘
   ```
   if a n aliasies are used the first n arrays as columns are returned :
   ```
   print 5, (t,w) = array_sort_asc(dynamic([2, 1, 3]), dynamic([20, 40, 30]), dynamic([100, 200]))
   ┌─5─┬─t───────┬─w──────────┐
   │ 5 │ [1,2,3] │ [40,20,30] │
   └───┴─────────┴────────────┘
   ```
## New Functions
- [case](https://learn.microsoft.com/en-us/azure/data-explorer/kusto/query/casefunction)  
   `Customers | extend t = case(Age <= 10, "A", Age <= 20, "B", Age <= 30, "C", "D");`
## Bug fixed
- [summarize crash if aggregation function is missing]
   ```
   fixed with throw exception:

   Exception on client:
   Code: 62. DB::Exception: Syntax error near keyword "by". (SYNTAX_ERROR)
   ```
- [make_datetime creates wrong date time]

- [todecimal() doesn't work with column arguments]



# October 25, 2022
## New Operators  
- [count](https://learn.microsoft.com/en-us/azure/data-explorer/kusto/query/countoperator)  
`Customers | count;`  
`Customers | where Age< 30 | count;`  
`Customers | where Age< 30 | limit 2 | count;`  
`Customers | where Age< 30 | limit 2 | count | project Count;`  

- [top](https://learn.microsoft.com/en-us/azure/data-explorer/kusto/query/topoperator)  
`Customers | top 3 by Age;`  
`Customers | top 3 by Age desc;`  
`Customers | top 3 by Age asc | order by FirstName;`  
`Customers | top 3 by FirstName  desc nulls first;`  
`Customers | top 3 by FirstName  desc nulls last;`  
`Customers | top 3 by Age | top 2 by FirstName;`  

- [top-hitters](https://learn.microsoft.com/en-us/azure/data-explorer/kusto/query/tophittersoperator)  
`Customers | top-hitters a = 2 of Age by extra;`  
`Customers | top-hitters 2 of Age;`  
`Customers | top-hitters 2 of Age by extra | top-hitters 2 of Age | order by Age;`  
`Customers | top-hitters 2 of Age by extra | where Age > 30;`  
`Customers | top-hitters 2 of Age by extra | where approximate_sum_extra < 200;`  
`Customers | top-hitters 2 of Age | where approximate_count_Age > 2;`  

## Bugs fixed
- [parse_version needs to return null when parameter is empty string]  
- [Different expressions with the same alias in function substring]  
- [parse_version needs to return null when parameter is empty string]  
- [parse_url() output mismatch for empty string]  
- [array_sum and array_length return incorrect results]  

# October 9, 2022  

## operator  
- [distinct](https://learn.microsoft.com/en-us/azure/data-explorer/kusto/query/distinctoperator)  
   `Customers | distinct *`  
   `Customers | distinct Occupation`  
   `Customers | distinct Occupation, Education`  
   `Customers | where Age <30 | distinct Occupation, Education`  
   `Customers | where Age <30 | order by Age| distinct Occupation, Education`  

## String functions  
- [reverse](https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/reversefunction)  
   `print reverse(123)`  
   `print reverse(123.34)`  
   `print reverse('clickhouse')`  
   `print reverse(3h)`  
   `print reverse(datetime(2017-1-1 12:23:34))`  

- [parse_command_line](https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/parse-command-line)  
   `print parse_command_line('echo \"hello world!\" print$?', \"Windows\")`  
  
- [parse_csv](https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/parsecsvfunction)  
  `print result=parse_csv('aa,b,cc')`  
  `print result_multi_record=parse_csv('record1,a,b,c\nrecord2,x,y,z')`  

- [parse_json](https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/parsejsonfunction)  
   `print parse_json( dynamic([1, 2, 3]))`  
   `print parse_json('{"a":123.5, "b":"{\\"c\\":456}"}')`  

- [extract_json](https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/extractjsonfunction)  
   `print extract_json( "$.a" , '{"a":123, "b":"{\\"c\\":456}"}' , typeof(int))`  

- [parse_version](https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/parse-versionfunction)  
 `print parse_version('1')`  
 `print parse_version('1.2.3.40')`  

## Bug fixed
- [correct array index in expression]  
   array index should start with 0  
- [Summarize should generate alias or use correct columns]
   - if bin is used , the column should be in select list if no alias include  
   - if no column included in aggregate functions,  ( like count() ), should has alias with fun name + '_',e.g  count_  
   - if column name included in aggregate functions, should have fun name + "_" + column name , like count(Age) -> count_Age  
   - if argument of an aggregate functions is an exprision, Columns1 ... Columnsn should be used as alias  
      ```
      Customers | summarize count() by bin(Age, 10)  
      ┌─Age─┬─count_─┐
      │  40 │      2 │
      │  20 │      6 │
      │  30 │      4 │
      └─────┴────────┘
      Customers | summarize count(Age) by bin(Age, 10)  
      ┌─Age─┬─count_Age─┐
      │  40 │         2 │
      │  20 │         6 │
      │  30 │         4 │
      └─────┴───────────┘
      Customers | summarize count(Age+1) by bin(Age+1, 10)
      ┌─Columns1─┬─count_─┐
      │       40 │      2 │
      │       20 │      6 │
      │       30 │      4 │
      └──────────┴────────┘
      ```
- [extend doesn't replace existing columns]  

- [throw exception if use quoted string as alias]  

- [repeat() doesn't work with count argument as negative value]  

- [substring() doesn't work right with negative offsets]  
- [endofmonth() doesn't return correct result]  

- [split() outputs array instead of string]  

- [split() returns empty string when arg goes out of bound]  

- [split() doesn't work with negative index]  


# September 26, 2022
## Bug fixed :  
["select * from kql" results in syntax error]  
[Parsing ipv4 with arrayStringConcat throws exception]  
[CH Client crashes on invalid function name]  
[extract() doesn't work right with 4th argument i.e typeof()]  
[parse_ipv6_mask return incorrect results]  
[timespan returns wrong output in seconds]  
[timespan doesn't work for nanoseconds and tick]  
[totimespan() doesn't work for nanoseconds and tick timespan unit]  
[data types should throw exception in certain cases]  
[decimal does not support scientific notation]  
[extend statement causes client core dumping]  
[extend crashes with array sorting]  
[Core dump happens when WHERE keyword doesn't follow field name]  
[Null values are missing in the result of `make_list_with_nulls']  
[trim functions use non-unique aliases]  
[format_ipv4_mask returns incorrect mask value]  

# September 12, 2022
## Extend operator
https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/extendoperator  
`T | extend T | extend duration = endTime - startTime`  
`T | project endTime, startTime | extend duration = endTime - startTime`
## Array functions
- [array_reverse](https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/array-reverse-function)  
   `print array_reverse(dynamic(["this", "is", "an", "example"])) == dynamic(["example","an","is","this"])`  

- [array_rotate_left](https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/array_rotate_leftfunction)  
   `print array_rotate_left(dynamic([1,2,3,4,5]), 2) == dynamic([3,4,5,1,2])`  
   `print array_rotate_left(dynamic([1,2,3,4,5]), -2) == dynamic([4,5,1,2,3])`  

- [array_rotate_right](https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/array_rotate_rightfunction)  
   `print array_rotate_right(dynamic([1,2,3,4,5]), -2) == dynamic([3,4,5,1,2])`  
   `print array_rotate_right(dynamic([1,2,3,4,5]), 2) == dynamic([4,5,1,2,3])`  

- [array_shift_left](https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/array_shift_leftfunction)  
   `print array_shift_left(dynamic([1,2,3,4,5]), 2) == dynamic([3,4,5,null,null])`  
   `print array_shift_left(dynamic([1,2,3,4,5]), -2) == dynamic([null,null,1,2,3])`  
   `print array_shift_left(dynamic([1,2,3,4,5]), 2, -1) == dynamic([3,4,5,-1,-1])`  
   `print array_shift_left(dynamic(['a', 'b', 'c']), 2) == dynamic(['c','',''])`  

- [array_shift_right](https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/array_shift_rightfunction)  
   `print array_shift_right(dynamic([1,2,3,4,5]), -2) == dynamic([3,4,5,null,null])`  
   `print array_shift_right(dynamic([1,2,3,4,5]), 2) == dynamic([null,null,1,2,3])`  
   `print array_shift_right(dynamic([1,2,3,4,5]), -2, -1) == dynamic([3,4,5,-1,-1])`  
   `print array_shift_right(dynamic(['a', 'b', 'c']), -2) == dynamic(['c','',''])`  

- [pack_array](https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/packarrayfunction)  
   `print x = 1, y = x * 2, z = y * 2, pack_array(x,y,z)`  

   Please note that only arrays of elements of the same type may be created at this time. The underlying reasons are explained under the release note section of the `dynamic` data type.

- [repeat](https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/repeatfunction)  
   `print repeat(1, 0) == dynamic([])`   
   `print repeat(1, 3) == dynamic([1, 1, 1])`  
   `print repeat("asd", 3) == dynamic(['asd', 'asd', 'asd'])`  
   `print repeat(timespan(1d), 3) == dynamic([86400, 86400, 86400])`  
   `print repeat(true, 3) == dynamic([true, true, true])`  

- [zip](https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/zipfunction)  
   `print zip(dynamic([1,3,5]), dynamic([2,4,6]))`  

   Please note that only arrays of the same type are supported in our current implementation. The underlying reasons are explained under the release note section of the `dynamic` data type.

## Data types
 - [dynamic](https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/scalar-data-types/dynamic)  
   `print isnull(dynamic(null))`  
   `print dynamic(1) == 1`  
   `print dynamic(timespan(1d)) == 86400`  
   `print dynamic([1, 2, 3])`  
   `print dynamic([[1], [2], [3]])`  
   `print dynamic(['a', "b", 'c'])`  

   According to the KQL specifications `dynamic` is a literal, which means that no function calls are permitted. Expressions producing literals such as `datetime` and `timespan` and their aliases (ie. `date` and `time`, respectively) along with nested `dynamic` literals are allowed.

   Please note that our current implementation supports only scalars and arrays made up of elements of the same type. Support for mixed types and property bags is deferred for now, based on our understanding of the required effort and discussion with representatives of the QRadar team.

## Mathematical functions  
 - [isnan](https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/isnanfunction)  
   `print isnan(double(nan)) == true`  
   `print isnan(4.2) == false`  
   `print isnan(4) == false`  
   `print isnan(real(+inf)) == false`  

## Set functions
Please note that functions returning arrays with set semantics may return them in any particular order, which may be subject to change in the future.

 - [jaccard_index](https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/jaccard-index-function)  
   `print jaccard_index(dynamic([1, 1, 2, 2, 3, 3]), dynamic([1, 2, 3, 4, 4, 4])) == 0.75`  
   `print jaccard_index(dynamic([1, 2, 3]), dynamic([])) == 0`  
   `print jaccard_index(dynamic([]), dynamic([1, 2, 3, 4])) == 0`  
   `print isnan(jaccard_index(dynamic([]), dynamic([])))`  
   `print jaccard_index(dynamic([1, 2, 3]), dynamic([4, 5, 6, 7])) == 0`  
   `print jaccard_index(dynamic(['a', 's', 'd']), dynamic(['f', 'd', 's', 'a'])) == 0.75`  
   `print jaccard_index(dynamic(['Chewbacca', 'Darth Vader', 'Han Solo']), dynamic(['Darth Sidious', 'Darth Vader'])) == 0.25`  

 - [set_difference](https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/setdifferencefunction)  
   `print set_difference(dynamic([1, 1, 2, 2, 3, 3]), dynamic([1, 2, 3])) == dynamic([])`  
   `print array_sort_asc(set_difference(dynamic([1, 4, 2, 3, 5, 4, 6]), dynamic([1, 2, 3])))[1] == dynamic([4, 5, 6])`  
   `print set_difference(dynamic([4]), dynamic([1, 2, 3])) == dynamic([4])`  
   `print array_sort_asc(set_difference(dynamic([1, 2, 3, 4, 5]), dynamic([5]), dynamic([2, 4])))[1] == dynamic([1, 3])`  
   `print array_sort_asc(set_difference(dynamic([1, 2, 3]), dynamic([])))[1] == dynamic([1, 2, 3])`  
   `print array_sort_asc(set_difference(dynamic(['a', 's', 'd']), dynamic(['a', 'f'])))[1] == dynamic(['d', 's'])`  
   `print array_sort_asc(set_difference(dynamic(['Chewbacca', 'Darth Vader', 'Han Solo']), dynamic(['Darth Sidious', 'Darth Vader'])))[1] == dynamic(['Chewbacca', 'Han Solo'])`  

 - [set_has_element](https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/sethaselementfunction)  
   `print set_has_element(dynamic(["this", "is", "an", "example"]), "example") == true`  
   `print set_has_element(dynamic(["this", "is", "an", "example"]), "examples") == false`  
   `print set_has_element(dynamic([1, 2, 3]), 2) == true`  
   `print set_has_element(dynamic([1, 2, 3, 4.2]), 4) == false`  

 - [set_intersect](https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/setintersectfunction)  
   `print array_sort_asc(set_intersect(dynamic([1, 1, 2, 2, 3, 3]), dynamic([1, 2, 3])))[1] == dynamic([1, 2, 3])`  
   `print array_sort_asc(set_intersect(dynamic([1, 4, 2, 3, 5, 4, 6]), dynamic([1, 2, 3])))[1] == dynamic([1, 2, 3])`  
   `print set_intersect(dynamic([4]), dynamic([1, 2, 3])) == dynamic([])`  
   `print set_intersect(dynamic([1, 2, 3, 4, 5]), dynamic([1, 3, 5]), dynamic([2, 5])) == dynamic([5])`  
   `print set_intersect(dynamic([1, 2, 3]), dynamic([])) == dynamic([])`  
   `print set_intersect(dynamic(['a', 's', 'd']), dynamic(['a', 'f'])) == dynamic(['a'])`  
   `print set_intersect(dynamic(['Chewbacca', 'Darth Vader', 'Han Solo']), dynamic(['Darth Sidious', 'Darth Vader'])) == dynamic(['Darth Vader'])`  

 - [set_union](https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/setunionfunction)  
   `print array_sort_asc(set_union(dynamic([1, 1, 2, 2, 3, 3]), dynamic([1, 2, 3])))[1] == dynamic([1, 2, 3])`  
   `print array_sort_asc(set_union(dynamic([1, 4, 2, 3, 5, 4, 6]), dynamic([1, 2, 3])))[1] == dynamic([1, 2, 3, 4, 5, 6])`  
   `print array_sort_asc(set_union(dynamic([4]), dynamic([1, 2, 3])))[1] == dynamic([1, 2, 3, 4])`  
   `print array_sort_asc(set_union(dynamic([1, 3, 4]), dynamic([5]), dynamic([2, 4])))[1] == dynamic([1, 2, 3, 4, 5])`  
   `print array_sort_asc(set_union(dynamic([1, 2, 3]), dynamic([])))[1] == dynamic([1, 2, 3])`  
   `print array_sort_asc(set_union(dynamic(['a', 's', 'd']), dynamic(['a', 'f'])))[1] == dynamic(['a', 'd', 'f', 's'])`  
   `print array_sort_asc(set_union(dynamic(['Chewbacca', 'Darth Vader', 'Han Solo']), dynamic(['Darth Sidious', 'Darth Vader'])))[1] == dynamic(['Chewbacca', 'Darth Sidious', 'Darth Vader', 'Han Solo'])`  

# August 29, 2022

## **mv-expand operator**
https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/mvexpandoperator
Note: **expand on array columns only**
- test cases 
   ```
   CREATE TABLE T
   (    
      a UInt8,
      b Array(String),
      c Array(Int8),
      d Array(Int8)
   ) ENGINE = Memory;

   INSERT INTO T VALUES (1, ['Salmon', 'Steak','Chicken'],[1,2,3,4],[5,6,7,8])
   
   T | mv-expand c  
   T | mv-expand c, d  
   T | mv-expand b | mv-expand c  
   T | mv-expand c to typeof(bool)  
   T | mv-expand with_itemindex=index b, c, d  
   T | mv-expand array_concat(c,d)   
   T | mv-expand x = c, y = d   
   T | mv-expand xy = array_concat(c, d)  
   T | mv-expand with_itemindex=index c,d to typeof(bool)  
   ```

## **make-series operator**  
https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/make-seriesoperator

- test case make-series on datetime column  
   ```
   CREATE TABLE T
   (    
      Supplier Nullable(String),
      Fruit String ,
      Price Float64,
      Purchase Date 
   ) ENGINE = Memory;

   INSERT INTO T VALUES  ('Aldi','Apple',4,'2016-09-10');
   INSERT INTO T VALUES  ('Costco','Apple',2,'2016-09-11');
   INSERT INTO T VALUES  ('Aldi','Apple',6,'2016-09-10');
   INSERT INTO T VALUES  ('Costco','Snargaluff',100,'2016-09-12');
   INSERT INTO T VALUES  ('Aldi','Apple',7,'2016-09-12');
   INSERT INTO T VALUES  ('Aldi','Snargaluff',400,'2016-09-11');
   INSERT INTO T VALUES  ('Costco','Snargaluff',104,'2016-09-12');
   INSERT INTO T VALUES  ('Aldi','Apple',5,'2016-09-12');
   INSERT INTO T VALUES  ('Aldi','Snargaluff',600,'2016-09-11');
   INSERT INTO T VALUES  ('Costco','Snargaluff',200,'2016-09-10');
   ```  
   Have from and to  
   ```
   T |  make-series PriceAvg = avg(Price) default=0 on Purchase from datetime(2016-09-10)  to datetime(2016-09-13) step 1d by Supplier, Fruit
   ```
   Has from , without to  
   ```
   T |  make-series PriceAvg = avg(Price) default=0 on Purchase from datetime(2016-09-10)  step 1d by Supplier, Fruit
   ```
   Without from , has to  
   ```
   T |  make-series PriceAvg = avg(Price) default=0 on Purchase  to datetime(2016-09-13) step 1d by Supplier, Fruit
   ```
   Without from , without to
   ```
   T |  make-series PriceAvg = avg(Price) default=0 on Purchase step 1d by Supplier, Fruit
   ```
   Without by clause
   ```
   T |  make-series PriceAvg = avg(Price) default=0 on Purchase step 1d
   ```
   Without aggregation alias
   ```
   T |  make-series avg(Price) default=0 on Purchase step 1d by Supplier, Fruit
   ```
   Has group expression alias
   ```
   T |  make-series avg(Price) default=0 on Purchase step 1d by Supplier_Name = Supplier, Fruit
   ```
   Use different step value
   ```
   T |  make-series PriceAvg = avg(Price) default=0 on Purchase from datetime(2016-09-10)  to datetime(2016-09-13) step 3d by Supplier, Fruit
   ```
- test case make-series on numeric column  
   ```
   CREATE TABLE T2
   (    
      Supplier Nullable(String),
      Fruit String ,
      Price Int32,
      Purchase Int32  
   ) ENGINE = Memory;

   INSERT INTO T2 VALUES  ('Aldi','Apple',4,10);
   INSERT INTO T2 VALUES  ('Costco','Apple',2,11);
   INSERT INTO T2 VALUES  ('Aldi','Apple',6,10);
   INSERT INTO T2 VALUES  ('Costco','Snargaluff',100,12);
   INSERT INTO T2 VALUES  ('Aldi','Apple',7,12);
   INSERT INTO T2 VALUES  ('Aldi','Snargaluff',400,11);
   INSERT INTO T2 VALUES  ('Costco','Snargaluff',104,12);
   INSERT INTO T2 VALUES  ('Aldi','Apple',5,12);
   INSERT INTO T2 VALUES  ('Aldi','Snargaluff',600,11);
   INSERT INTO T2 VALUES  ('Costco','Snargaluff',200,10);
   ```
   Have from and to  
   ```
   T2 | make-series PriceAvg=avg(Price) default=0 on Purchase from 10 to  15 step  1.0  by Supplier, Fruit;
   ```
   Has from , without to  
   ```
   T2 | make-series PriceAvg=avg(Price) default=0 on Purchase from 10 step  1.0  by Supplier, Fruit;
   ```
   Without from , has to  
   ```
   T2 | make-series PriceAvg=avg(Price) default=0 on Purchase to 18 step  4.0  by Supplier, Fruit;
   ```
   Without from , without to  
   ```
   T2 | make-series PriceAvg=avg(Price) default=0 on Purchase step  2.0  by Supplier, Fruit;
   ```
   Without by clause
   ```
   T2 | make-series PriceAvg=avg(Price) default=0 on Purchase step  2.0;
   ```

## Aggregate Functions
- [bin](https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/binfunction)  
   `print bin(4.5, 1)`  
   `print bin(time(16d), 7d)`  
   `print bin(datetime(1970-05-11 13:45:07), 1d)`  
- [stdev](https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/stdev-aggfunction)  
   `Customers | summarize t = stdev(Age) by FirstName`  

- [stdevif](https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/stdevif-aggfunction)  
   `Customers | summarize t = stdevif(Age, Age < 10) by FirstName`  

- [binary_all_and](https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/binary-all-and-aggfunction)  
   `Customers | summarize t = binary_all_and(Age) by FirstName`  

- [binary_all_or](https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/binary-all-or-aggfunction)  
   `Customers | summarize t = binary_all_or(Age) by FirstName`  

- [binary_all_xor](https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/binary-all-xor-aggfunction)  
   `Customers | summarize t = binary_all_xor(Age) by FirstName`  

- [percentiles](https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/percentiles-aggfunction)  
   `Customers | summarize percentiles(Age, 30, 40, 50, 60, 70) by FirstName`  

- [percentilesw](https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/percentiles-aggfunction)  
   `DataTable | summarize t = percentilesw(Bucket, Frequency, 50, 75, 99.9)`  

- [percentile](https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/percentiles-aggfunction)  
   `Customers | summarize t = percentile(Age, 50) by FirstName`  

- [percentilew](https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/percentiles-aggfunction)  
   `DataTable | summarize t = percentilew(Bucket, Frequency, 50)`  

## Dynamic functions
- [array_sort_asc](https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/arraysortascfunction)  
   **Only support the constant dynamic array.**  
   **Returns an array. So, each element of the input has to be of same datatype.**  
   `print t = array_sort_asc(dynamic([null, 'd', 'a', 'c', 'c']))`  
   `print t = array_sort_asc(dynamic([4, 1, 3, 2]))`  
   `print t = array_sort_asc(dynamic(['b', 'a', 'c']), dynamic(['q', 'p', 'r']))`  
   `print t = array_sort_asc(dynamic(['q', 'p', 'r']), dynamic(['clickhouse','hello', 'world']))`  
   `print t = array_sort_asc( dynamic(['d', null, 'a', 'c', 'c']) , false)`  
   `print t = array_sort_asc( dynamic(['d', null, 'a', 'c', 'c']) , 1 > 2)`  
   `print t = array_sort_asc( dynamic([null, 'd', null, null, 'a', 'c', 'c', null, null, null]) , false)`  
   `print t = array_sort_asc( dynamic([null, null, null]) , false)`  
   `print t = array_sort_asc(dynamic([2, 1, null,3]), dynamic([20, 10, 40, 30]), 1 > 2)`  
   `print t = array_sort_asc(dynamic([2, 1, null,3]), dynamic([20, 10, 40, 30, 50, 3]), 1 > 2)`  

- [array_sort_desc](https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/arraysortdescfunction) **(only support the constant dynamic array)**  
   
   `print t = array_sort_desc(dynamic([null, 'd', 'a', 'c', 'c']))`  
   `print t = array_sort_desc(dynamic([4, 1, 3, 2]))`  
   `print t = array_sort_desc(dynamic(['b', 'a', 'c']), dynamic(['q', 'p', 'r']))`  
   `print t = array_sort_desc(dynamic(['q', 'p', 'r']), dynamic(['clickhouse','hello', 'world']))`  
   `print t = array_sort_desc( dynamic(['d', null, 'a', 'c', 'c']) , false)`  
   `print t = array_sort_desc( dynamic(['d', null, 'a', 'c', 'c']) , 1 > 2)`  
   `print t = array_sort_desc( dynamic([null, 'd', null, null, 'a', 'c', 'c', null, null, null]) , false)`  
   `print t = array_sort_desc( dynamic([null, null, null]) , false)`  
   `print t = array_sort_desc(dynamic([2, 1, null, 3]), dynamic([20, 10, 40, 30]), 1 > 2)`  
   `print t = array_sort_desc(dynamic([2, 1, null,3, null]), dynamic([20, 10, 40, 30, 50, 3]), 1 > 2)`  

- [array_concat](https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/arrayconcatfunction)  
   `print array_concat(dynamic([1, 2, 3]), dynamic([4, 5]), dynamic([6, 7, 8, 9])) == dynamic([1, 2, 3, 4, 5, 6, 7, 8, 9])`  

- [array_iff / array_iif](https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/arrayifffunction)  
   `print array_iif(dynamic([true, false, true]), dynamic([1, 2, 3]), dynamic([4, 5, 6])) == dynamic([1, 5, 3])`  
   `print array_iif(dynamic([true, false, true]), dynamic([1, 2, 3, 4]), dynamic([4, 5, 6])) == dynamic([1, 5, 3])`  
   `print array_iif(dynamic([true, false, true, false]), dynamic([1, 2, 3, 4]), dynamic([4, 5, 6])) == dynamic([1, 5, 3, null])`  
   `print array_iif(dynamic([1, 0, -1, 44, 0]), dynamic([1, 2, 3, 4]), dynamic([4, 5, 6])) == dynamic([1, 5, 3, 4, null])`  
   `print t = array_iif(dynamic([true, false, true, false, true]), dynamic([1.1, 2.2, 3.3, 4.4, 5.5]), 999.99);`
   `print t = array_iif(dynamic([true, false, true, false, true]), 90, dynamic([1, 3]));`

- [array_slice](https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/arrayslicefunction)  
   `print array_slice(dynamic([1,2,3]), 1, 2) == dynamic([2, 3])`  
   `print array_slice(dynamic([1,2,3,4,5]), 2, -1) == dynamic([3, 4, 5])`  
   `print array_slice(dynamic([1,2,3,4,5]), -3, -2) == dynamic([3, 4])`  

- [array_split](https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/arraysplitfunction)  
   `print array_split(dynamic([1,2,3,4,5]), 2) == dynamic([[1,2],[3,4,5]])`  
   `print array_split(dynamic([1,2,3,4,5]), dynamic([1,3])) == dynamic([[1],[2,3],[4,5]])`  

## DateTimeFunctions

- [ago](https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/agofunction)  
   `print ago(2h)`  

- [endofday](https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/endofdayfunction)  
   `print endofday(datetime(2017-01-01 10:10:17), -1)`  
   `print endofday(datetime(2017-01-01 10:10:17), 1)`  
   `print endofday(datetime(2017-01-01 10:10:17))`  

- [endofmonth](https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/endofmonthfunction)  
   `print endofmonth(datetime(2017-01-01 10:10:17), -1)`  
   `print endofmonth(datetime(2017-01-01 10:10:17), 1)`  
   `print endofmonth(datetime(2017-01-01 10:10:17))`  

- [endofweek](https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/endofweekfunction)  
   `print endofweek(datetime(2017-01-01 10:10:17), 1)`  
   `print endofweek(datetime(2017-01-01 10:10:17), -1)`  
   `print endofweek(datetime(2017-01-01 10:10:17))`  

- [endofyear](https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/endofyearfunction)  
   `print endofyear(datetime(2017-01-01 10:10:17), -1)`  
   `print endofyear(datetime(2017-01-01 10:10:17), 1)`  
   `print endofyear(datetime(2017-01-01 10:10:17))`  

- [make_datetime](https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/make-datetimefunction)  
   `print make_datetime(2017,10,01)`  
   `print make_datetime(2017,10,01,12,10)`  
   `print make_datetime(2017,10,01,12,11,0.1234567)`  

-  [datetime_diff](https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/datetime-difffunction)  
   `print datetime_diff('year',datetime(2017-01-01),datetime(2000-12-31))`  
   `print datetime_diff('quarter',datetime(2017-07-01),datetime(2017-03-30))`  
   `print datetime_diff('minute',datetime(2017-10-30 23:05:01),datetime(2017-10-30 23:00:59))`  

- [unixtime_microseconds_todatetime](https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/unixtime-microseconds-todatetimefunction)  
   `print unixtime_microseconds_todatetime(1546300800000000)`  

- [unixtime_milliseconds_todatetime](https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/unixtime-milliseconds-todatetimefunction)  
   `print unixtime_milliseconds_todatetime(1546300800000)`  

- [unixtime_nanoseconds_todatetime](https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/unixtime-nanoseconds-todatetimefunction)  
   `print unixtime_nanoseconds_todatetime(1546300800000000000)`  

- [datetime_part](https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/datetime-partfunction)  
   `print datetime_part('day', datetime(2017-10-30 01:02:03.7654321))`  

- [datetime_add](https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/datetime-addfunction)  
   `print datetime_add('day',1,datetime(2017-10-30 01:02:03.7654321))`  

- [format_timespan](https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/format-timespanfunction)  
   `print format_timespan(time(1d), 'd-[hh:mm:ss]')`  
   `print format_timespan(time('12:30:55.123'), 'ddddd-[hh:mm:ss.ffff]')`  

- [format_datetime](https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/format-datetimefunction)  
   `print format_datetime(todatetime('2009-06-15T13:45:30.6175425'), 'yy-M-dd [H:mm:ss.fff]')`  
   `print format_datetime(datetime(2015-12-14 02:03:04.12345), 'y-M-d h:m:s tt')`  

- [todatetime](https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/todatetimefunction)  
   `print  todatetime('2014-05-25T08:20:03.123456Z')`  
   `print  todatetime('2014-05-25 20:03.123')`  

- [totimespan] (https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/totimespanfunction)
   ` print totimespan('0.01:34:23')`
   `print totimespan(1d)`

# August 15, 2022
   **double quote support**  
   ``print res = strcat("double ","quote")``  
## Aggregate functions
 - [bin_at](https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/binatfunction)  
   `print res = bin_at(6.5, 2.5, 7)`  
   `print res = bin_at(1h, 1d, 12h)`  
   `print res = bin_at(datetime(2017-05-15 10:20:00.0), 1d, datetime(1970-01-01 12:00:00.0))`  
   `print res = bin_at(datetime(2017-05-17 10:20:00.0), 7d, datetime(2017-06-04 00:00:00.0))`  

 - [array_index_of](https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/arrayindexoffunction)  
    *Supports only basic lookup. Do not support start_index, length and occurrence*  
    `print output = array_index_of(dynamic(['John', 'Denver', 'Bob', 'Marley']), 'Marley')`  
    `print output = array_index_of(dynamic([1, 2, 3]), 2)`  
 - [array_sum](https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/array-sum-function)  
    `print output = array_sum(dynamic([2, 5, 3]))`  
    `print output = array_sum(dynamic([2.5, 5.5, 3]))`  
 - [array_length](https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/arraylengthfunction)  
    `print output = array_length(dynamic(['John', 'Denver', 'Bob', 'Marley']))`  
    `print output = array_length(dynamic([1, 2, 3]))`

## Conversion
- [tobool / toboolean](https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/toboolfunction)
   `print tobool(true) == true`
   `print toboolean(false) == false`
   `print tobool(0) == false`
   `print toboolean(19819823) == true`
   `print tobool(-2) == true`
   `print isnull(toboolean('a'))`
   `print tobool('true') == true`
   `print toboolean('false') == false`

- [todouble / toreal](https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/todoublefunction)
   `print todouble(4) == 4`
   `print toreal(4.2) == 4.2`
   `print isnull(todouble('a'))`
   `print toreal('-0.3') == -0.3`

- [toint](https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/tointfunction)
   `print isnull(toint('a'))`  
   `print toint(4) == 4`  
   `print toint('4') == 4`  
   `print isnull(toint(4.2))`  

- [tostring](https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/tostringfunction)
   `print tostring(123) == '123'`  
   `print tostring('asd') == 'asd'`  

## Data Types
 - [dynamic](https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/scalar-data-types/dynamic)  
    *Supports only 1D array*  
    `print output = dynamic(['a', 'b', 'c'])`  
    `print output = dynamic([1, 2, 3])`  
 
- [bool,boolean](https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/scalar-data-types/bool)  
   `print bool(1)`  
   `print boolean(0)`  

- [datetime](https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/scalar-data-types/datetime)  
   `print datetime(2015-12-31 23:59:59.9)`  
   `print datetime('2015-12-31 23:59:59.9')`  
   `print datetime("2015-12-31:)`  

- [guid](https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/scalar-data-types/guid)  
   `print guid(74be27de-1e4e-49d9-b579-fe0b331d3642)`  
   `print guid('74be27de-1e4e-49d9-b579-fe0b331d3642')`  
   `print guid('74be27de1e4e49d9b579fe0b331d3642')`  

- [int](https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/scalar-data-types/int)  
   `print int(1)`  

- [long](https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/scalar-data-types/long)  
   `print long(16)`  

- [real](https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/scalar-data-types/real)  
   `print real(1)`  

- [timespan ,time](https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/scalar-data-types/timespan)  
   **Note** the timespan is used for calculating datatime, so the output is in seconds. e.g. time(1h) = 3600
   `print 1d`  
   `print 30m`  
   `print time('0.12:34:56.7')`  
   `print time(2h)`  
   `print timespan(2h)`  


## StringFunctions
 
- [base64_encode_fromguid](https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/base64-encode-fromguid-function)  
`print Quine = base64_encode_fromguid('ae3133f2-6e22-49ae-b06a-16e6a9b212eb')`  
- [base64_decode_toarray](https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/base64_decode_toarrayfunction)  
`print base64_decode_toarray('S3VzdG8=')`  
- [base64_decode_toguid](https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/base64-decode-toguid-function)  
`print base64_decode_toguid('YWUzMTMzZjItNmUyMi00OWFlLWIwNmEtMTZlNmE5YjIxMmVi')`  
- [replace_regex](https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/replace-regex-function)  
`print replace_regex('Hello, World!', '.', '\\0\\0')`  
- [has_any_index](https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/has-any-index-function)  
`print idx = has_any_index('this is an example', dynamic(['this', 'example']))`
- [translate](https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/translatefunction)  
`print translate('krasp', 'otsku', 'spark')`  
- [trim](https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/trimfunction)  
`print trim('--', '--https://bing.com--')`  
- [trim_end](https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/trimendfunction)  
`print trim_end('.com', 'bing.com')`  
- [trim_start](https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/trimstartfunction)  
`print trim_start('[^\\w]+', strcat('-  ','Te st1','// $'))`  

## DateTimeFunctions
- [startofyear](https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/startofyearfunction)  
   `print startofyear(datetime(2017-01-01 10:10:17), -1)`  
   `print startofyear(datetime(2017-01-01 10:10:17), 0)`  
   `print startofyear(datetime(2017-01-01 10:10:17), 1)`  
- [weekofyear](https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/weekofyearfunction)  
   `print week_of_year(datetime(2020-12-31))`  
   `print week_of_year(datetime(2020-06-15))`  
   `print week_of_year(datetime(1970-01-01))`  
   `print  week_of_year(datetime(2000-01-01))`  

- [startofweek](https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/startofweekfunction)  
   `print startofweek(datetime(2017-01-01 10:10:17), -1)`  
   `print startofweek(datetime(2017-01-01 10:10:17), 0)`  
   `print startofweek(datetime(2017-01-01 10:10:17), 1)`  

- [startofmonth](https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/startofmonthfunction)  
   `print startofmonth(datetime(2017-01-01 10:10:17), -1)`  
   `print startofmonth(datetime(2017-01-01 10:10:17), 0)`  
   `print startofmonth(datetime(2017-01-01 10:10:17), 1)`  

- [startofday](https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/startofdayfunction)  
   `print startofday(datetime(2017-01-01 10:10:17), -1)`  
   `print startofday(datetime(2017-01-01 10:10:17), 0)`  
   `print startofday(datetime(2017-01-01 10:10:17), 1)`  

- [monthofyear](https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/monthofyearfunction)  
   `print monthofyear(datetime("2015-12-14"))`  

- [hourofday](https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/hourofdayfunction)  
   `print hourofday(datetime(2015-12-14 18:54:00))`  

- [getyear](https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/getyearfunction)  
   `print getyear(datetime(2015-10-12))`  

- [getmonth](https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/getmonthfunction)  
   `print getmonth(datetime(2015-10-12))`  

- [dayofyear](https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/dayofyearfunction)  
   `print dayofyear(datetime(2015-12-14))`  

- [dayofmonth](https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/dayofmonthfunction)  
   `print (datetime(2015-12-14))`  

- [unixtime_seconds_todatetime](https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/unixtime-seconds-todatetimefunction)  
   `print unixtime_seconds_todatetime(1546300800)`  

- [dayofweek](https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/dayofweekfunction)  
   `print dayofweek(datetime(2015-12-20))`  

- [now](https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/nowfunction)  
   `print now()`  
   `print now(2d)`  
   `print now(-2h)`  
   `print now(5microseconds)`  
   `print now(5seconds)`  
   `print now(6minutes)`  
   `print now(-2d) `  
   `print now(time(1d))`  


## Binary functions
- [binary_and](https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/binary-andfunction)  
   `print binary_and(15, 3) == 3`  
   `print binary_and(1, 2) == 0`  
- [binary_not](https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/binary-notfunction)  
   `print binary_not(1) == -2`  
- [binary_or](https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/binary-orfunction)  
   `print binary_or(3, 8) == 11`  
   `print binary_or(1, 2) == 3`  
- [binary_shift_left](https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/binary-shift-leftfunction)  
   `print binary_shift_left(1, 1) == 2`  
   `print binary_shift_left(1, 64) == 1`  
- [binary_shift_right](https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/binary-shift-rightfunction)  
   `print binary_shift_right(1, 1) == 0`
   `print binary_shift_right(1, 64) == 1`
- [binary_xor](https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/binary-xorfunction)  
   `print binary_xor(1, 3) == 2`  
- [bitset_count_ones](https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/bitset-count-onesfunction)  
   `print bitset_count_ones(42) == 3`  

## IP functions
- [format_ipv4](https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/format-ipv4-function)  
   `print format_ipv4('192.168.1.255', 24) == '192.168.1.0'`  
   `print format_ipv4(3232236031, 24) == '192.168.1.0'`  
- [format_ipv4_mask](https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/format-ipv4-mask-function)  
   `print format_ipv4_mask('192.168.1.255', 24) == '192.168.1.0/24'`  
   `print format_ipv4_mask(3232236031, 24) == '192.168.1.0/24'`  
- [ipv4_compare](https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/ipv4-comparefunction)  
   `print ipv4_compare('127.0.0.1', '127.0.0.1') == 0`  
   `print ipv4_compare('192.168.1.1', '192.168.1.255') < 0`  
   `print ipv4_compare('192.168.1.1/24', '192.168.1.255/24') == 0`  
   `print ipv4_compare('192.168.1.1', '192.168.1.255', 24) == 0`  
- [ipv4_is_match](https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/ipv4-is-matchfunction)  
   `print ipv4_is_match('127.0.0.1', '127.0.0.1') == true`  
   `print ipv4_is_match('192.168.1.1', '192.168.1.255') == false`  
   `print ipv4_is_match('192.168.1.1/24', '192.168.1.255/24') == true`  
   `print ipv4_is_match('192.168.1.1', '192.168.1.255', 24) == true`  
- [ipv6_compare](https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/ipv6-comparefunction)  
   `print ipv6_compare('::ffff:7f00:1', '127.0.0.1') == 0`  
   `print ipv6_compare('fe80::85d:e82c:9446:7994', 'fe80::85d:e82c:9446:7995') < 0`  
   `print ipv6_compare('192.168.1.1/24', '192.168.1.255/24') == 0`  
   `print ipv6_compare('fe80::85d:e82c:9446:7994/127', 'fe80::85d:e82c:9446:7995/127') == 0`  
   `print ipv6_compare('fe80::85d:e82c:9446:7994', 'fe80::85d:e82c:9446:7995', 127) == 0`  
- [ipv6_is_match](https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/ipv6-is-matchfunction)  
   `print ipv6_is_match('::ffff:7f00:1', '127.0.0.1') == true`  
   `print ipv6_is_match('fe80::85d:e82c:9446:7994', 'fe80::85d:e82c:9446:7995') == false`  
   `print ipv6_is_match('192.168.1.1/24', '192.168.1.255/24') == true`  
   `print ipv6_is_match('fe80::85d:e82c:9446:7994/127', 'fe80::85d:e82c:9446:7995/127') == true`  
   `print ipv6_is_match('fe80::85d:e82c:9446:7994', 'fe80::85d:e82c:9446:7995', 127) == true`  
- [parse_ipv4_mask](https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/parse-ipv4-maskfunction)  
   `print parse_ipv4_mask('127.0.0.1', 24) == 2130706432`  
   `print parse_ipv4_mask('192.1.168.2', 31) == 3221334018`  
   `print parse_ipv4_mask('192.1.168.3', 31) == 3221334018`  
   `print parse_ipv4_mask('127.2.3.4', 32) == 2130838276`  
- [parse_ipv6_mask](https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/parse-ipv6-maskfunction)  
   `print parse_ipv6_mask('127.0.0.1', 24) == '0000:0000:0000:0000:0000:ffff:7f00:0000'`  
   `print parse_ipv6_mask('fe80::85d:e82c:9446:7994', 120) == 'fe80:0000:0000:0000:085d:e82c:9446:7900'`  

# August 1, 2022

**The config setting to allow modify dialect setting**.
   - Set dialect setting in  server configuration XML at user level(` users.xml `). This sets the ` dialect ` at server startup and CH will do query parsing for all users with ` default ` profile according to dialect value.

   For example:
   ` <profiles>
        <!-- Default settings. -->
        <default>
            <load_balancing>random</load_balancing>
            <dialect>kusto</dialect>
        </default> `
   
   - Query can be executed with HTTP client as below once dialect is set in users.xml
      ` echo "KQL query" | curl -sS "http://localhost:8123/?" --data-binary @- `
   
   - To execute the query using clickhouse-client , Update clickhouse-client.xml as below and connect clickhouse-client with --config-file option (` clickhouse-client --config-file=<config-file path> `) 

     ` <config>
         <dialect>kusto</dialect>
      </config>  `

   OR 
      pass dialect setting with '--'. For example : 
      ` clickhouse-client --dialect='kusto' -q "KQL query" `

- **strcmp** (https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/strcmpfunction)  
   `print strcmp('abc','ABC')`

- **parse_url** (https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/parseurlfunction)  
   `print Result = parse_url('scheme://username:password@www.google.com:1234/this/is/a/path?k1=v1&k2=v2#fragment')`

- **parse_urlquery** (https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/parseurlqueryfunction)  
   `print Result = parse_urlquery('k1=v1&k2=v2&k3=v3')`

- **print operator** (https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/printoperator)  
   `print x=1, s=strcat('Hello', ', ', 'World!')`

- **Aggregate Functions:**
 - [make_list()](https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/makelist-aggfunction)  
   `Customers | summarize t = make_list(FirstName) by FirstName`
   `Customers | summarize t = make_list(FirstName, 10) by FirstName`
 - [make_list_if()](https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/makelistif-aggfunction)  
   `Customers | summarize t = make_list_if(FirstName, Age > 10) by FirstName`
   `Customers | summarize t = make_list_if(FirstName, Age > 10, 10) by FirstName`
 - [make_list_with_nulls()](https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/make-list-with-nulls-aggfunction)  
   `Customers | summarize t = make_list_with_nulls(Age) by FirstName`
 - [make_set()](https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/makeset-aggfunction)  
   `Customers | summarize t = make_set(FirstName) by FirstName`
   `Customers | summarize t = make_set(FirstName, 10) by FirstName`
 - [make_set_if()](https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/makesetif-aggfunction)  
   `Customers | summarize t = make_set_if(FirstName, Age > 10) by FirstName`
   `Customers | summarize t = make_set_if(FirstName, Age > 10, 10) by FirstName`

## IP functions

- **The following functions now support arbitrary expressions as their argument:**
   - [ipv4_is_private](https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/ipv4-is-privatefunction)
   - [ipv4_is_in_range](https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/ipv4-is-in-range-function)
   - [ipv4_netmask_suffix](https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/ipv4-netmask-suffix-function)
      
# July 17, 2022

## Renamed dialect from sql_dialect to dialect

`set dialect='clickhouse'`  
`set dialect='kusto'`  

## IP functions
- [parse_ipv4](https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/parse-ipv4function)
   `"Customers | project parse_ipv4('127.0.0.1')"`
- [parse_ipv6](https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/parse-ipv6function)
   `"Customers | project parse_ipv6('127.0.0.1')"`

Please note that the functions listed below only take constant parameters for now. Further improvement is to be expected to support expressions.

- [ipv4_is_private](https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/ipv4-is-privatefunction)
   `"Customers | project ipv4_is_private('192.168.1.6/24')"`
   `"Customers | project ipv4_is_private('192.168.1.6')"`
- [ipv4_is_in_range](https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/ipv4-is-in-range-function)
   `"Customers | project ipv4_is_in_range('127.0.0.1', '127.0.0.1')"`
   `"Customers | project ipv4_is_in_range('192.168.1.6', '192.168.1.1/24')"`
- [ipv4_netmask_suffix](https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/ipv4-netmask-suffix-function)
   `"Customers | project ipv4_netmask_suffix('192.168.1.1/24')"`
   `"Customers | project ipv4_netmask_suffix('192.168.1.1')"`

## string functions
- **support subquery for `in` orerator** (https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/in-cs-operator)  
 (subquery need to be wrapped with bracket inside bracket)

    `Customers | where Age in ((Customers|project Age|where Age < 30))`
 Note: case-insensitive not supported yet
- **has_all**  (https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/has-all-operator)  
    `Customers|where Occupation has_any ('Skilled','abcd')`  
     note : subquery not supported yet
- **has _any**  (https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/has-anyoperator)  
    `Customers|where Occupation has_all ('Skilled','abcd')`  
    note : subquery not supported yet
- **countof**  (https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/countoffunction)  
   `Customers | project countof('The cat sat on the mat', 'at')`  
   `Customers | project countof('The cat sat on the mat', 'at', 'normal')`  
   `Customers | project countof('The cat sat on the mat', 'at', 'regex')`
- **extract**  ( https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/extractfunction)  
`Customers | project extract('(\\b[A-Z]+\\b).+(\\b\\d+)', 0, 'The price of PINEAPPLE ice cream is 20')`  
`Customers | project extract('(\\b[A-Z]+\\b).+(\\b\\d+)', 1, 'The price of PINEAPPLE ice cream is 20')`  
`Customers | project extract('(\\b[A-Z]+\\b).+(\\b\\d+)', 2, 'The price of PINEAPPLE ice cream is 20')`  
`Customers | project extract('(\\b[A-Z]+\\b).+(\\b\\d+)', 3, 'The price of PINEAPPLE ice cream is 20')`  
`Customers | project extract('(\\b[A-Z]+\\b).+(\\b\\d+)', 2, 'The price of PINEAPPLE ice cream is 20', typeof(real))` 

- **extract_all** (https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/extractallfunction)  

    `Customers | project extract_all('(\\w)(\\w+)(\\w)','The price of PINEAPPLE ice cream is 20')`  
    note:  captureGroups not supported yet

- **split** (https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/splitfunction)  
    `Customers | project split('aa_bb', '_')`  
    `Customers | project split('aaa_bbb_ccc', '_', 1)`  
    `Customers | project split('', '_')`  
    `Customers | project split('a__b', '_')`  
    `Customers | project split('aabbcc', 'bb')`  

- **strcat_delim**  (https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/strcat-delimfunction)  
    `Customers | project strcat_delim('-', '1', '2', 'A') , 1s)`  
    `Customers | project strcat_delim('-', '1', '2', strcat('A','b'))`  
    note: only support string now.

- **indexof** (https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/indexoffunction)  
    `Customers | project indexof('abcdefg','cde')`  
    `Customers | project indexof('abcdefg','cde',2)`  
    `Customers | project indexof('abcdefg','cde',6)`  
    note: length and occurrence not supported yet




# July 4, 2022

## sql_dialect

- default is `clickhouse`  
    `set sql_dialect='clickhouse'`
- only process kql  
    `set sql_dialect='kusto'`

## KQL() function

 - create table  
  `CREATE TABLE kql_table4 ENGINE = Memory AS select *, now() as new_column From kql(Customers | project LastName,Age);`  
   verify the content of `kql_table`  
    `select * from kql_table`
   
 - insert into table  
    create a tmp table:
    ```
    CREATE TABLE temp
    (    
        FirstName Nullable(String),
        LastName String, 
        Age Nullable(UInt8)
    ) ENGINE = Memory;
    ```
    `INSERT INTO temp select * from kql(Customers|project FirstName,LastName,Age);`  
    verify the content of `temp`   
        `select * from temp`

 - Select from kql()  
    `Select * from kql(Customers|project FirstName)`

## KQL operators:
 - Tabular expression statements  
    `Customers`
 - Select Column  
    `Customers | project FirstName,LastName,Occupation`
 - Limit returned results  
    `Customers | project FirstName,LastName,Occupation | take 1 | take 3`
 - sort, order  
    `Customers | order by Age desc , FirstName asc`
 - Filter   
    `Customers | where Occupation == 'Skilled Manual'`
 - summarize  
    `Customers |summarize  max(Age) by Occupation`

## KQL string operators and functions
 - contains  
    `Customers |where Education contains  'degree'`
 - !contains  
    `Customers |where Education !contains  'degree'`
 - contains_cs  
    `Customers |where Education contains  'Degree'`
 - !contains_cs  
    `Customers |where Education !contains  'Degree'`
 - endswith  
    `Customers | where FirstName endswith 'RE'`
 - !endswith  
     `Customers | where !FirstName endswith 'RE'`
 - endswith_cs  
 `Customers | where FirstName endswith_cs  're'`
 - !endswith_cs  
  `Customers | where FirstName !endswith_cs  're'`
 - ==  
    `Customers | where Occupation == 'Skilled Manual'`
 - !=  
    `Customers | where Occupation != 'Skilled Manual'`
 - has  
    `Customers | where Occupation has 'skilled'`
 - !has  
    `Customers | where Occupation !has 'skilled'`
 - has_cs  
    `Customers | where Occupation has 'Skilled'`
 - !has_cs  
    `Customers | where Occupation !has 'Skilled'`
 - hasprefix  
    `Customers | where Occupation hasprefix_cs 'Ab'`
 - !hasprefix  
    `Customers | where Occupation !hasprefix_cs 'Ab'`
 - hasprefix_cs  
    `Customers | where Occupation hasprefix_cs 'ab'`
 - !hasprefix_cs  
    `Customers | where Occupation! hasprefix_cs 'ab'`
 - hassuffix  
    `Customers | where Occupation hassuffix 'Ent'`
 - !hassuffix  
    `Customers | where Occupation !hassuffix 'Ent'`
 - hassuffix_cs  
    `Customers | where Occupation hassuffix 'ent'`
 - !hassuffix_cs  
    `Customers | where Occupation hassuffix 'ent'`
 - in  
    `Customers |where Education in ('Bachelors','High School')`
 - !in  
    `Customers |  where Education !in  ('Bachelors','High School')`
 - matches regex  
    `Customers | where FirstName matches regex 'P.*r'`
 - startswith  
    `Customers | where FirstName startswith 'pet'`
 - !startswith  
    `Customers | where FirstName !startswith 'pet'`
 - startswith_cs  
    `Customers | where FirstName startswith_cs 'pet'`
 - !startswith_cs  
     `Customers | where FirstName !startswith_cs 'pet'`

 - base64_encode_tostring()  
 `Customers | project base64_encode_tostring('Kusto1') | take 1`
 - base64_decode_tostring()  
 `Customers | project base64_decode_tostring('S3VzdG8x') | take 1`
  - isempty()  
 `Customers | where  isempty(LastName)`
 - isnotempty()  
  `Customers | where  isnotempty(LastName)`
 - isnotnull()  
  `Customers | where  isnotnull(FirstName)`
 - isnull()  
 `Customers | where  isnull(FirstName)`
 - url_decode()  
 `Customers | project url_decode('https%3A%2F%2Fwww.test.com%2Fhello%20word') | take 1`
 - url_encode()  
    `Customers | project url_encode('https://www.test.com/hello word') | take 1`
 - substring()  
    `Customers | project name_abbr = strcat(substring(FirstName,0,3), ' ', substring(LastName,2))`
 - strcat()  
    `Customers | project name = strcat(FirstName, ' ', LastName)`
 - strlen()  
    `Customers | project FirstName, strlen(FirstName)`
 - strrep()  
    `Customers | project strrep(FirstName,2,'_')`
 - toupper()  
    `Customers | project toupper(FirstName)`
 - tolower()  
    `Customers | project tolower(FirstName)`

 ## Aggregate Functions
 - arg_max()
 - arg_min()
 - avg()
 - avgif()
 - count()
 - countif()
 - max()
 - maxif()
 - min()
 - minif()
 - sum()
 - sumif()
 - dcount()
 - dcountif()
 - bin
