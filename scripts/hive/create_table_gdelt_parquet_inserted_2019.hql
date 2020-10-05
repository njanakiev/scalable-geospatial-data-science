-- Stage-Stage-1: Map: 33   Cumulative CPU: 1159.19 sec   
-- HDFS Read: 8741342688 HDFS Write: 5773063226 SUCCESS                         
-- Total MapReduce CPU Time Spent: 19 minutes 19 seconds 190 msec
-- Time taken: 216.519 seconds                                                                                                         
-- real    3m53.013s
-- user    0m45.301s
-- sys     0m3.031s

DROP TABLE IF EXISTS gdelt_parquet_inserted_2019;

CREATE EXTERNAL TABLE gdelt_parquet_inserted_2019 (
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
LOCATION 'hdfs://node-master:54310/user/hadoop/gdelt_parquet_inserted_2019/';

INSERT INTO gdelt_parquet_inserted_2019
SELECT * FROM gdelt_csv_2019;
