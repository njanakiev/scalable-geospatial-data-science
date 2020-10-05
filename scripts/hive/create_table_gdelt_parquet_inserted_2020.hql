-- Stage-Stage-1: Map: 15   Cumulative CPU: 492.47 sec   
-- HDFS Read: 3946117781 HDFS Write: 2586409744 SUCCESS
-- Total MapReduce CPU Time Spent: 8 minutes 12 seconds 470 msec
-- Time taken: 136.567 seconds
-- real    2m34.696s
-- user    0m44.079s
-- sys     0m2.783s


DROP TABLE IF EXISTS gdelt_parquet_inserted_2020;

CREATE EXTERNAL TABLE gdelt_parquet_inserted_2020 (
    `event_id`        BIGINT,
    `date`            DATE,
    `event_date`      DATE,
    `event_code`      INT,
    `event_base_code` INT,
    `event_root_code` INT,
    `lat`             DOUBLE,
    `lon`             DOUBLE,
    `source_url`      STRING
)
STORED AS PARQUET
LOCATION 'hdfs://node-master:54310/user/hadoop/gdelt_parquet_inserted_2020/';

INSERT INTO gdelt_parquet_inserted_2020
SELECT * FROM gdelt_csv_2020;
