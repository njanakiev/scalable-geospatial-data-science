DROP TABLE IF EXISTS gdelt_csv_2020;

CREATE EXTERNAL TABLE gdelt_csv_2020 (
    `event_id`        BIGINT,
    `date`            DATE,
    `event_date`      DATE,
    `event_code`      INT,
    `event_base_code` INT,
    `event_root_code` INT,
    `lat`             DECIMAL(18,14),
    `lon`             DECIMAL(18,14),
    `source_url`      VARCHAR(100)
) ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
LOCATION 'hdfs://node-master:54310/user/hadoop/gdelt_csv_2020/';



DROP TABLE IF EXISTS gdelt_csv_2019;

CREATE EXTERNAL TABLE gdelt_csv_2019 (
    `event_id`        BIGINT,
    `date`            DATE,
    `event_date`      DATE,
    `event_code`      INT,
    `event_base_code` INT,
    `event_root_code` INT,
    `lat`             DECIMAL(18,14),
    `lon`             DECIMAL(18,14),
    `source_url`      VARCHAR(100)
) ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
LOCATION 'hdfs://node-master:54310/user/hadoop/gdelt_csv_2019/';
