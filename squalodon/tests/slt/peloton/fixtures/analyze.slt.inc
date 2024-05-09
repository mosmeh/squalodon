statement ok
DROP TABLE IF EXISTS test;

statement ok
CREATE TABLE test(a INT PRIMARY KEY, b INT, c INT, d VARCHAR);

statement ok
INSERT INTO test VALUES
    (1, 22, 333, 'abcd'),
    (2, 22, 333, 'abc'),
    (3, 11, 222, 'abcd');
