statement ok
DROP TABLE IF EXISTS __mock_table_1;

statement ok
CREATE TABLE __mock_table_1 (colA INTEGER, colB INTEGER);

statement ok
INSERT INTO __mock_table_1 VALUES
    (0, 0),
    (1, 100),
    (2, 200),
    (3, 300),
    (4, 400),
    (5, 500),
    (6, 600),
    (7, 700),
    (8, 800),
    (9, 900),
    (10, 1000),
    (11, 1100),
    (12, 1200),
    (13, 1300),
    (14, 1400),
    (15, 1500),
    (16, 1600),
    (17, 1700),
    (18, 1800),
    (19, 1900),
    (20, 2000),
    (21, 2100),
    (22, 2200),
    (23, 2300),
    (24, 2400),
    (25, 2500),
    (26, 2600),
    (27, 2700),
    (28, 2800),
    (29, 2900),
    (30, 3000),
    (31, 3100),
    (32, 3200),
    (33, 3300),
    (34, 3400),
    (35, 3500),
    (36, 3600),
    (37, 3700),
    (38, 3800),
    (39, 3900),
    (40, 4000),
    (41, 4100),
    (42, 4200),
    (43, 4300),
    (44, 4400),
    (45, 4500),
    (46, 4600),
    (47, 4700),
    (48, 4800),
    (49, 4900),
    (50, 5000),
    (51, 5100),
    (52, 5200),
    (53, 5300),
    (54, 5400),
    (55, 5500),
    (56, 5600),
    (57, 5700),
    (58, 5800),
    (59, 5900),
    (60, 6000),
    (61, 6100),
    (62, 6200),
    (63, 6300),
    (64, 6400),
    (65, 6500),
    (66, 6600),
    (67, 6700),
    (68, 6800),
    (69, 6900),
    (70, 7000),
    (71, 7100),
    (72, 7200),
    (73, 7300),
    (74, 7400),
    (75, 7500),
    (76, 7600),
    (77, 7700),
    (78, 7800),
    (79, 7900),
    (80, 8000),
    (81, 8100),
    (82, 8200),
    (83, 8300),
    (84, 8400),
    (85, 8500),
    (86, 8600),
    (87, 8700),
    (88, 8800),
    (89, 8900),
    (90, 9000),
    (91, 9100),
    (92, 9200),
    (93, 9300),
    (94, 9400),
    (95, 9500),
    (96, 9600),
    (97, 9700),
    (98, 9800),
    (99, 9900);

statement ok
DROP TABLE IF EXISTS __mock_table_tas_2024;

statement ok
CREATE TABLE __mock_table_tas_2024 (github_id VARCHAR, office_hour VARCHAR);

statement ok
INSERT INTO __mock_table_tas_2024 VALUES
    ('AlSchlo', 'Friday'),
    ('walkingcabbages', 'Thursday'),
    ('averyqi115', 'Friday'),
    ('lanlou1554', 'Wednesday'),
    ('sweetsuro', 'Thursday'),
    ('ChaosZhai', 'Yesterday'),
    ('SDTheSlayer', 'Monday'),
    ('xx01cyx', 'Tuesday'),
    ('yliang412', 'Tuesday'),
    ('thelongmarch-azx', 'Monday');

statement ok
DROP TABLE IF EXISTS __mock_table_schedule;

statement ok
CREATE TABLE __mock_table_schedule (
    day_of_week VARCHAR,
    has_lecture INTEGER
);

statement ok
INSERT INTO __mock_table_schedule VALUES
    ('Monday', 1),
    ('Tuesday', 0),
    ('Wednesday', 1),
    ('Thursday', 0),
    ('Friday', 0),
    ('Saturday', 0),
    ('Sunday', 0);

statement ok
DROP TABLE IF EXISTS __mock_agg_input_small;

statement ok
CREATE TABLE __mock_agg_input_small (v1 INTEGER, v2 INTEGER, v3 INTEGER, v4 INTEGER, v5 INTEGER, v6 VARCHAR);

statement ok
INSERT INTO __mock_agg_input_small
SELECT (i + 2) % 10, i, (i + 50) % 100, i / 100, 233, repeat('💩', (i % 8) + 1)
FROM generate_series(0, 999) as s (i);

statement ok
DROP TABLE IF EXISTS __mock_agg_input_big;

statement ok
CREATE TABLE __mock_agg_input_big (v1 INTEGER, v2 INTEGER, v3 INTEGER, v4 INTEGER, v5 INTEGER, v6 VARCHAR);

statement ok
INSERT INTO __mock_agg_input_big
SELECT (i + 2) % 10, i, (i + 50) % 100, i / 1000, 233, repeat('💩', (i % 16) + 1)
FROM generate_series(0, 9999) as s (i);

statement ok
DROP TABLE IF EXISTS __mock_graph;

statement ok
CREATE TABLE __mock_graph(src int, dst int, src_label VARCHAR, dst_label VARCHAR, distance int);

statement ok
INSERT INTO __mock_graph VALUES
    (0, 0, '000', '000', NULL),
    (1, 0, '001', '000', 1),
    (2, 0, '002', '000', 1),
    (3, 0, '003', '000', 1),
    (4, 0, '004', '000', 1),
    (5, 0, '005', '000', 1),
    (6, 0, '006', '000', 1),
    (7, 0, '007', '000', 1),
    (8, 0, '008', '000', 1),
    (9, 0, '009', '000', 1),
    (0, 1, '000', '001', 1),
    (1, 1, '001', '001', NULL),
    (2, 1, '002', '001', 1),
    (3, 1, '003', '001', 1),
    (4, 1, '004', '001', 1),
    (5, 1, '005', '001', 1),
    (6, 1, '006', '001', 1),
    (7, 1, '007', '001', 1),
    (8, 1, '008', '001', 1),
    (9, 1, '009', '001', 1),
    (0, 2, '000', '002', 1),
    (1, 2, '001', '002', 1),
    (2, 2, '002', '002', NULL),
    (3, 2, '003', '002', 1),
    (4, 2, '004', '002', 1),
    (5, 2, '005', '002', 1),
    (6, 2, '006', '002', 1),
    (7, 2, '007', '002', 1),
    (8, 2, '008', '002', 1),
    (9, 2, '009', '002', 1),
    (0, 3, '000', '003', 1),
    (1, 3, '001', '003', 1),
    (2, 3, '002', '003', 1),
    (3, 3, '003', '003', NULL),
    (4, 3, '004', '003', 1),
    (5, 3, '005', '003', 1),
    (6, 3, '006', '003', 1),
    (7, 3, '007', '003', 1),
    (8, 3, '008', '003', 1),
    (9, 3, '009', '003', 1),
    (0, 4, '000', '004', 1),
    (1, 4, '001', '004', 1),
    (2, 4, '002', '004', 1),
    (3, 4, '003', '004', 1),
    (4, 4, '004', '004', NULL),
    (5, 4, '005', '004', 1),
    (6, 4, '006', '004', 1),
    (7, 4, '007', '004', 1),
    (8, 4, '008', '004', 1),
    (9, 4, '009', '004', 1),
    (0, 5, '000', '005', 1),
    (1, 5, '001', '005', 1),
    (2, 5, '002', '005', 1),
    (3, 5, '003', '005', 1),
    (4, 5, '004', '005', 1),
    (5, 5, '005', '005', NULL),
    (6, 5, '006', '005', 1),
    (7, 5, '007', '005', 1),
    (8, 5, '008', '005', 1),
    (9, 5, '009', '005', 1),
    (0, 6, '000', '006', 1),
    (1, 6, '001', '006', 1),
    (2, 6, '002', '006', 1),
    (3, 6, '003', '006', 1),
    (4, 6, '004', '006', 1),
    (5, 6, '005', '006', 1),
    (6, 6, '006', '006', NULL),
    (7, 6, '007', '006', 1),
    (8, 6, '008', '006', 1),
    (9, 6, '009', '006', 1),
    (0, 7, '000', '007', 1),
    (1, 7, '001', '007', 1),
    (2, 7, '002', '007', 1),
    (3, 7, '003', '007', 1),
    (4, 7, '004', '007', 1),
    (5, 7, '005', '007', 1),
    (6, 7, '006', '007', 1),
    (7, 7, '007', '007', NULL),
    (8, 7, '008', '007', 1),
    (9, 7, '009', '007', 1),
    (0, 8, '000', '008', 1),
    (1, 8, '001', '008', 1),
    (2, 8, '002', '008', 1),
    (3, 8, '003', '008', 1),
    (4, 8, '004', '008', 1),
    (5, 8, '005', '008', 1),
    (6, 8, '006', '008', 1),
    (7, 8, '007', '008', 1),
    (8, 8, '008', '008', NULL),
    (9, 8, '009', '008', 1),
    (0, 9, '000', '009', 1),
    (1, 9, '001', '009', 1),
    (2, 9, '002', '009', 1),
    (3, 9, '003', '009', 1),
    (4, 9, '004', '009', 1),
    (5, 9, '005', '009', 1),
    (6, 9, '006', '009', 1),
    (7, 9, '007', '009', 1),
    (8, 9, '008', '009', 1),
    (9, 9, '009', '009', NULL);

statement ok
DROP TABLE IF EXISTS __mock_table_123;

statement ok
CREATE TABLE __mock_table_123 (number INTEGER);

statement ok
INSERT INTO __mock_table_123 VALUES (1), (2), (3);

statement ok
DROP TABLE IF EXISTS test_simple_seq_1;

statement ok
CREATE TABLE test_simple_seq_1 (col1 INTEGER NOT NULL);

statement ok
INSERT INTO test_simple_seq_1 VALUES
    (0), (1), (2), (3), (4), (5), (6), (7), (8), (9);

statement ok
DROP TABLE IF EXISTS test_simple_seq_2;

statement ok
CREATE TABLE test_simple_seq_2 (col1 INTEGER NOT NULL, col2 INTEGER NOT NULL);

statement ok
INSERT INTO test_simple_seq_2 VALUES
    (0, 10), (1, 11), (2, 12), (3, 13), (4, 14), (5, 15), (6, 16), (7, 17), (8, 18), (9, 19);
