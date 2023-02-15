DROP TABLE IF EXISTS IP_STRING;
DROP TABLE IF EXISTS IP_ARRAY;

CREATE TABLE IP_STRING (haystack String, needle String) ENGINE = Memory;
CREATE TABLE IP_ARRAY (haystack String, needle Array(String)) ENGINE = Memory;
INSERT INTO IP_STRING (haystack, needle) VALUES ('09:46:00 10.0.0.1 GET /favicon.ico 404', '10.0.0.1'), ('09:46:00 10.0.0.300 1.2.3.4 GET /favicon.ico 404', '10.0.0.300'), ('09:46:0010.0.0.1 GET /favicon.ico 404', '10.0.0.1'), ('09:46:00 10.0.0.1GET /favicon.ico 404', '10.0.0.1'), ('09:46:00 10.0.0.1 192.168.1.1 GET /favicon.ico 404', '192.168.1.1');
INSERT INTO IP_ARRAY  (haystack, needle) VALUES ('09:46:00 10.0.0.1 GET /favicon.ico 404', ['1.2.3.4', '2.3.4.5', '10.0.0.1']), ('09:46:00 10.0.0.1 GET /favicon.ico 404', ['1.2.3.', '2.3.4.', '10.0.0.']);

set dialect='kusto';
print has_ipv4('X'); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
print has_any_ipv4('X'); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
print has_ipv4_prefix('X'); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
print has_any_ipv4_prefix('X'); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }

print has_ipv4(1, 2); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
print has_any_ipv4(1,2); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
print has_ipv4_prefix(1,2); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
print has_any_ipv4_prefix(1,2); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

print has_ipv4('X', 2); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
print has_any_ipv4('X', 2); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
print has_ipv4_prefix('X', 2); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
print has_any_ipv4_prefix('X', 2); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

IP_STRING | extend has_ipv4(haystack, needle);
IP_STRING | extend has_any_ipv4(haystack, needle);
IP_STRING | extend has_any_ipv4(haystack, needle, '1.2.3.4');
IP_STRING | extend has_any_ipv4(haystack, 'X', needle);
IP_STRING | extend has_ipv4_prefix(haystack, needle);
IP_STRING | extend has_ipv4_prefix(haystack, substring(needle, 0, strlen(needle)-1)); 
IP_STRING | extend has_ipv4_prefix(haystack, substring(needle, 0, strlen(needle)-2));
IP_ARRAY | extend has_any_ipv4(haystack, dynamic(needle));
IP_ARRAY | extend has_any_ipv4_prefix(haystack, dynamic(needle));
set dialect='kusto_auto';
DROP TABLE IP_STRING;
DROP TABLE IP_ARRAY;

