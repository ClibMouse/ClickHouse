DROP TABLE IF EXISTS events;
CREATE TABLE events
(    
 data_source_name String, 
 original_time Date,
 low_level_categories Array(Int64),
 user_id UInt8
) ENGINE = Memory;

INSERT INTO events VALUES ('abcd1','2024-03-01',[11,12,13,14,15],1),('abcd1','2024-03-02',[11,12,13,14,15],1),('abcd1','2024-03-03',[11,12,13,14,15],1),('abcd2','2024-03-01',[11,12,13,14,15],1),('abcd2','2024-03-02',[11,12,13,14,15],1),('abcd2','2024-03-03',[11,12,13,14,15],1);

DROP TABLE IF EXISTS make_series_test_table;
CREATE TABLE make_series_test_table
(
   Supplier Nullable(String),
   Fruit String,
   Price Float64,
   Purchase Date
) ENGINE = Memory;
INSERT INTO make_series_test_table VALUES  ('Aldi','Apple',4,'2016-09-10'), ('Aldi','Apple',6,'2016-09-10'), ('Aldi','Apple',7,'2016-09-12'), ('Aldi','Apple',400,'2016-09-11'), ('Aldi','Apple',5,'2016-09-12');

set dialect = 'kusto';

print '--Simple function call--';
print '> series_decompose_anomalies';
print series_decompose_anomalies(dynamic([1,0.02,0.4,0.55,0.56,0.8]));
print (a)       = series_decompose_anomalies(dynamic([1,0.02,4,55,56,8]));
print (a, b)    = series_decompose_anomalies(dynamic([1,0.022,4,55,56,8]));
print (a, b, c) = series_decompose_anomalies(dynamic([1,0.022,4,55,56,8]));

print '> series_decompose';
print series_decompose(dynamic([10.1, 20.45, 40.34, 10.1, 20.45, 40.34, 10.1, 20.45, 40.34]));
print (a)          = series_decompose(dynamic([10.1, 20.45, 40.34, 10.1, 20.45, 40.34, 10.1, 20.45, 40.34]));
print (a, b)       = series_decompose(dynamic([10.1, 20.45, 40.34, 10.1, 20.45, 40.34, 10.1, 20.45, 40.34]));
print (a, b, c)    = series_decompose(dynamic([10.1, 20.45, 40.34, 10.1, 20.45, 40.34, 10.1, 20.45, 40.34]));
print (a, b, c, d) = series_decompose(dynamic([10.1, 20.45, 40.34, 10.1, 20.45, 40.34, 10.1, 20.45, 40.34]));

print '> series_outliers';
print series_outliers(dynamic([-3, 2, 15, 3, 5, 6, 4.50, 5, 12, 45, 12, 3.40, 3, 4, 5, 6]));
print a = series_outliers(dynamic([-3, 2, 15, 3, 5, 6, 4.50, 5, 12, 45, 12, 3.40, 3, 4, 5, 6]), 'tukey',0,0.2,0.8);
print outliers = series_outliers(dynamic([-3, 2, 15, 3, 5, 6, 4.50, 5, 12, 45, 12, 3.40, 3, 4, 5, 6]), 'ctukey',0,0.2,0.8);

print '> series_periods_detect';
print series_periods_detect(dynamic([80, 139, 87, 110, 68, 54, 50, 51, 53, 133, 86, 141, 97, 156, 94, 149, 95, 140, 77, 61, 50, 54, 47, 133, 72, 152, 94, 148, 105, 162, 101, 160, 87, 63, 53, 55, 54, 151, 103, 189, 108, 183, 113, 175, 113, 178, 90, 71, 62, 62, 65, 165, 109, 181, 115, 182, 121, 178, 114, 170]));

print '--Piped function call--';
print '> series_decompose_anomalies';
events
| project data_source_name, original_time
| make-series alert_count=count() default=0 on original_time step 1h by data_source_name
| project (anomalies, score, baseline) = series_decompose_anomalies(alert_count, 5, -1, 'linefit', 0, "ctukey", 1);

print '> series_decompose';
make_series_test_table
| make-series PriceAvg = avg(Price) default=0 on Purchase from datetime(2016-09-10) to datetime(2016-09-14) step 1d by Supplier, Fruit
| project series_decompose(PriceAvg);

print '> series_outliers';
range x from 0 to 364 step 1
| extend y = iff(x % 10 == 0, x*100, x)
| summarize series = make_list(y)
| project outliers = series_outliers(series);

print '> series_periods_detect';
range x from 0 to 100 step 1
| project y = x % 11
| project y = iff(y % 10, y*2, y)
| summarize y = make_list(y)
| project series_periods_detect(y)


-- Erroneous aliases
-- print (a, b,  ) = series_decompose_anomalies(dynamic([1,2,4,55,56,8]));
-- print (a,  , c) = series_decompose_anomalies(dynamic([1,2,4,55,56,8]));
-- print ( , b, c) = series_decompose_anomalies(dynamic([1,2,4,55,56,8]));

-- print (a,  ,  ) = series_decompose_anomalies(dynamic([1,2,4,55,56,8]));
-- print ( , b,  ) = series_decompose_anomalies(dynamic([1,2,4,55,56,8]));
-- print ( ,  , c) = series_decompose_anomalies(dynamic([1,2,4,55,56,8]));

-- print (,,) = series_decompose_anomalies(dynamic([1,2,4,55,56,8]));
-- print (a,b,c,d) = series_decompose_anomalies(dynamic([1,2,4,55,56,8]));
-- print (a,b,c,) = series_decompose_anomalies(dynamic([1,2,4,55,56,8]));
