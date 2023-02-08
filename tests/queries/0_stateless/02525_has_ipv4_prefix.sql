SELECT 'tests';
DROP TABLE IF EXISTS has_ipv4_tbl;
CREATE TABLE has_ipv4_tbl (ip String) ENGINE = Memory;
INSERT INTO has_ipv4_tbl (ip) VALUES ('192.168.1.');
INSERT INTO has_ipv4_tbl (ip) VALUES ('127.0.0.');
SELECT ip, kql_has_ipv4_prefix('XXXX 127.0.0.1', ip) from has_ipv4_tbl;
DROP TABLE has_ipv4_tbl;

