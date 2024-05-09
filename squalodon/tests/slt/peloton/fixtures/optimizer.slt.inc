statement ok
DROP TABLE IF EXISTS test;

statement ok
CREATE TABLE test(a INT PRIMARY KEY, b INT, c INT);

statement ok
INSERT INTO test VALUES
    (1, 22, 333),
    (2, 11, 000),
    (3, 33, 444),
    (4, 00, 555);

statement ok
DROP TABLE IF EXISTS test2;

statement ok
DROP TABLE IF EXISTS agg;
