statement ok
DROP TABLE IF EXISTS test;

statement ok
CREATE TABLE test(s SMALLINT, i INTEGER, bi BIGINT, r REAL, t TEXT, bp BPCHAR, vc VARCHAR, ts TEXT);

statement ok
INSERT INTO test VALUES
    (1, 1, 1, 1.0, 'text1', '1', 'varchar1', '2016-06-22 19:10:21-00'),
    (2, 2, 2, 2.0, 'text2', '2', 'varchar2', '2016-06-22 19:10:22-00'),
    (3, 3, 3, 3.0, 'text3', '3', 'varchar3', '2016-06-22 19:10:23-00'),
    (1, 1, 1, 1.0, 'text1', '1', 'varchar1', '2016-06-22 19:10:21-00'),
    (2, 2, 2, 2.0, 'text2', '2', 'varchar2', '2016-06-22 19:10:22-00'),
    (3, 3, 3, 3.0, 'text3', '3', 'varchar3', '2016-06-22 19:10:23-00'),
    (1, 1, 1, 1.0, 'text1', '1', 'varchar1', '2016-06-22 19:10:21-00'),
    (2, 2, 2, 2.0, 'text2', '2', 'varchar2', '2016-06-22 19:10:22-00'),
    (3, 3, 3, 3.0, 'text3', '3', 'varchar3', '2016-06-22 19:10:23-00');
