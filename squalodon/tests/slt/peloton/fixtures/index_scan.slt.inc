statement ok
DROP TABLE IF EXISTS test;

statement ok
CREATE TABLE test(a INT, b INT, c INT, d VARCHAR);

statement ok
INSERT INTO test VALUES
    (1, 22, 333, 'abcd'),
    (2, 33, 111, 'bcda'),
    (3, 11, 222, 'bcd');
