DROP TABLE IF EXISTS ne_10_states_provinces_parquet;

CREATE EXTERNAL TABLE ne_10_states_provinces_parquet (
    `ne_id`     BIGINT,
    `name`      STRING,
    `iso_a2`    STRING,
    `geometry`  BINARY
) 
STORED AS PARQUET
LOCATION 'hdfs://node-master:54310/user/hadoop/ne_parquet';
