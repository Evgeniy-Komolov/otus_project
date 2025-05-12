CREATE database otus;

CREATE TABLE otus.example_table_ss_clickhouse (
    id UInt32,
    name String,
    value Float32,
    created_at DateTime
) ENGINE = MergeTree()
ORDER BY id;

INSERT INTO otus.example_table_ss_clickhouse (id, name, value, created_at) VALUES
(1, 'A', 10.5, '2023-01-01 00:00:00'),
(2, 'B', 20.0, '2023-01-02 00:00:00'),
(3, 'C', 30.5, '2023-01-03 00:00:00'),
(4, 'D', 40.0, '2023-01-04 00:00:00'),
(5, 'E', 50.5, '2023-01-05 00:00:00'),
(1, 'A', 20.5, '2023-01-06 00:00:00'),
(2, 'B', 30.0, '2023-01-07 00:00:00'),
(3, 'C', 10.5, '2023-01-08 00:00:00'),
(4, 'D', 45.0,'2023-01-09 00:00:00'),
(5, 'E', 33.5, '2023-01-10 00:00:00');


CREATE TABLE otus.example_table_backup AS otus.example_table_ss_clickhouse;



INSERT INTO FUNCTION s3('http://minio:9000/otus-project/backup/example_table_backup.csv.gz',
                         'otus_clickhouse',
                         'qwert1234',
                         'CSV',
                         'id UInt32, name String, value Float32, created_at DateTime',
                         'gzip')
SELECT * FROM otus.example_table_ss_clickhouse;



-- INSERT INTO otus.example_table_backup
SELECT * FROM s3('http://minio:9000/otus-project/backup/example_table_backup.csv.gz',
                           'otus_clickhouse',
                           'qwert1234',
                           'CSV',
                           'id UInt32, name String, value Float32, created_at DateTime',
                           'gzip');

SELECT *
FROM s3('http://minio:9000/otus-project/backup/example_table_backup.csv.gz',
        'my_s3_profile',
        'CSV',
        'id UInt32, name String, value Float32, created_at DateTime',
        'gzip');


SELECT version();

-- INSERT INTO otus.example_table_backup
SELECT * FROM s3('http://minio:9000/otus-project/backup/example_table_backup.csv.gz',
                 'CSV',
                 'id UInt32, name String, value Float32, created_at DateTime',
                 'gzip');


SELECT *
FROM s3(
    'http://minio:9000/otus-project/backup/example_table_backup.csv.gz',
    'my_s3_profile',
    'CSV',
    'id UInt32, name String, value Float32, created_at DateTime',
    'gzip'
);

SELECT *
FROM s3(
    'http://minio:9000/otus-project/backup/example_table_backup.csv.gz',
    'my_s3_profile',
    'CSV',
    'id UInt32, name String, value Float32, created_at DateTime',
    'gzip'
);