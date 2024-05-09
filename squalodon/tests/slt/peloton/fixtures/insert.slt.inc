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
CREATE TABLE test2(a INT PRIMARY KEY, b INT, c INT);

statement ok
INSERT INTO test2 VALUES
    (5, 99, 888),
    (6, 88, 777),
    (7, 77, 666),
    (8, 55, 999);

statement ok
DROP TABLE IF EXISTS test3;

statement ok
CREATE TABLE test3(a INT, b CHAR, c VARCHAR);

statement ok
DROP TABLE IF EXISTS test4;

statement ok
CREATE TABLE test4(a INT PRIMARY KEY, b INTEGER, c SMALLINT, d BIGINT, e DECIMAL, f REAL, g TEXT, i CHAR, j VARCHAR, k BYTEA, l BOOLEAN);

statement ok
INSERT INTO test4 VALUES
    (1, 2, 3, 4, 5.1, 6.1, '2017-10-10 00:00:00-00', 'A', 'a', '1', 'true'),
    (11, 12, 13, 14, 15.1, 16.1, '2017-10-11 00:00:00-00', 'B', 'b', '2', 'false');

statement ok
DROP TABLE IF EXISTS test5;

statement ok
CREATE TABLE test5(a INT PRIMARY KEY, b INTEGER, c SMALLINT, d BIGINT, e DECIMAL, f REAL, g TEXT, i CHAR, j VARCHAR, k BYTEA, l BOOLEAN);

statement ok
DROP TABLE IF EXISTS test6;

statement ok
CREATE TABLE test6(a INT, b INT, c INT);

statement ok
INSERT INTO test6 VALUES
    (1, 22, 333),
    (2, 11, 000),
    (3, 33, 444),
    (4, 00, 555);

statement ok
DROP TABLE IF EXISTS test7;

statement ok
CREATE TABLE test7(a INT, b INT, c INT);

statement ok
INSERT INTO test7 VALUES
    (99, 5, 888),
    (88, 6, 777),
    (77, 7, 666),
    (55, 8, 999);

statement ok
DROP TABLE IF EXISTS test8;

statement ok
CREATE TABLE test8(num1 int, num2 int, num3 int not null);
