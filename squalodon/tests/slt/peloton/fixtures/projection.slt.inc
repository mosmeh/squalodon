statement ok
DROP TABLE IF EXISTS test;

statement ok
CREATE TABLE test(a INTEGER, b SMALLINT, c INT, d BIGINT, e DECIMAL);

statement ok
INSERT INTO test VALUES (1, 2, 3, 4, 5.0);
