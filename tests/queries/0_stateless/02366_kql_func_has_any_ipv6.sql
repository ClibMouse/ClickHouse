DROP TABLE IF EXISTS IP_STRING;
DROP TABLE IF EXISTS IP_ARRAY;

CREATE TABLE IP_STRING (haystack String, needle String) ENGINE = Memory;
CREATE TABLE IP_ARRAY (haystack String, needle Array(String)) ENGINE = Memory;
INSERT INTO IP_STRING (haystack, needle) VALUES ('09:46:00 2600:1404:6400:1695::1e89 GET /favicon.ico 404', '2600:1404:6400:1695:0:0:0:1e89'), ('09:46:00 ::ffff:1.2.3.4 GET /favicon.ico 404', '2600:1404:6400:1695:0:0:0:1e89'), ('09:46:002600:1404:6400:1695::1e89 GET /favicon.ico 404', '2600:1404:6400:1695:0:0:0:1e89'), ('09:46:00 2600:1404:6400:1695::1e89GET /favicon.ico 404', '2600:1404:6400:1695:0:0:0:1e89'), ('09:46:00 2600:1404:6400:168a::1e89 2600:1404:6400:1695::1e89 GET /favicon.ico 404', '2600:1404:6400:1695:0:0:0:1e89');
INSERT INTO IP_ARRAY  (haystack, needle) VALUES ('09:46:00 2600:1404:6400:1695::1e89 GET /favicon.ico 404', ['2500:1404:6400:1695:0:0:0:1e89', '2600:1404:6400:1695:0:0:0:1e89']), ('09:46:00 2600:1404:6400:1695:0:0:0:1e89 GET /favicon.ico 404', ['2400:1404:6400:1695:', '2500:1404:6400:1695', '2600:1404:6400:1695:']);

set dialect='kusto';
print has_ipv6('X'); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
print has_any_ipv6('X'); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
print has_ipv6_prefix('X'); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
print has_any_ipv6_prefix('X'); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }

print has_ipv6(1, 2); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
print has_any_ipv6(1,2); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
print has_ipv6_prefix(1,2); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
print has_any_ipv6_prefix(1,2); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

print has_ipv6('X', 2); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
print has_any_ipv6('X', 2); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
print has_ipv6_prefix('X', 2); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
print has_any_ipv6_prefix('X', 2); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

print "-- #1 --";
IP_STRING | extend has_ipv6(haystack, needle);
print "-- #2 --";
IP_STRING | extend has_any_ipv6(haystack, needle);
print "-- #3 --";
IP_STRING | extend has_any_ipv6(haystack, needle, '0:0:0:0:0:ffff:1.2.4.5');
print "-- #4 --";
IP_STRING | extend has_any_ipv6(haystack, 'X', needle);
print "-- #5 --";
IP_STRING | extend has_ipv6_prefix(haystack, needle);
print "-- #6 --";
IP_STRING | extend has_ipv6_prefix(haystack, substring(needle, 0, strlen(needle)-4)); 
print "-- #7 --";
IP_STRING | extend has_ipv6_prefix(haystack, substring(needle, 0, strlen(needle)-5));
print "-- #8 --";
IP_ARRAY | extend has_any_ipv6(haystack, dynamic(needle));
print "-- #9 --";
IP_ARRAY | extend has_any_ipv6_prefix(haystack, dynamic(needle));
set dialect='kusto_auto';
DROP TABLE IP_STRING;
DROP TABLE IP_ARRAY;

