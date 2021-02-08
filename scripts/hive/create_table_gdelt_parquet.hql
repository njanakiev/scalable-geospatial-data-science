SET parquet.compression=SNAPPY;

DROP TABLE IF EXISTS gdelt_parquet;
DROP TABLE IF EXISTS gdelt_parquet_2020;

CREATE EXTERNAL TABLE gdelt_parquet (
    `event_id`        STRING,
    `date`            DATE,
    `event_date`      DATE,
    `event_code`      BIGINT,
    `event_base_code` BIGINT,
    `event_root_code` BIGINT,
    `lat`             DOUBLE,
    `lon`             DOUBLE,
    `geo_type`        BIGINT,
    `country_code`    STRING,
    `adm1_code`       STRING,
    `source_url`      STRING,
    `netloc`          STRING
) 
STORED AS PARQUET
LOCATION 'hdfs://node-master:54310/user/hadoop/gdelt_500MB.snappy.parq';


CREATE EXTERNAL TABLE gdelt_parquet_2020 (
    `event_id`        STRING,
    `date`            DATE,
    `event_date`      DATE,
    `event_code`      BIGINT,
    `event_base_code` BIGINT,
    `event_root_code` BIGINT,
    `lat`             DOUBLE,
    `lon`             DOUBLE,
    `geo_type`        BIGINT,
    `country_code`    STRING,
    `adm1_code`       STRING,
    `source_url`      STRING,
    `netloc`          STRING
) 
STORED AS PARQUET
LOCATION 'hdfs://node-master:54310/user/hadoop/gdelt_2020_500MB.snappy.parq';
