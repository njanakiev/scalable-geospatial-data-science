DROP TABLE IF EXISTS gdelt_parquet_2020;

CREATE EXTERNAL TABLE gdelt_parquet_2020 (
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
LOCATION 'hdfs://node-master:54310/user/hadoop/gdelt_parquet_2020/2020.snappy.parq';


-- Failed with exception java.io.IOException:org.apache.hadoop.hive.ql.metadata.HiveException: java.lang.ClassCastException: org.apache.hadoop.io.Text cannot be cast to org.apache.hadoop.hive.serde2.io.DateWritableV2
