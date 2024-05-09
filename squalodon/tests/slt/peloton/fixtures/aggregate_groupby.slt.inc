statement ok
DROP TABLE IF EXISTS test;

statement ok
CREATE TABLE test(a INT PRIMARY KEY, b INT, c INT, d INT);

statement ok
INSERT INTO test VALUES
    (1, 2, 3, 1),
    (2, 2, 3, 1),
    (3, 2, 6, 1),
    (4, 2, 3, 2),
    (5, 3, 6, 2),
    (6, 4, 6, 2);
