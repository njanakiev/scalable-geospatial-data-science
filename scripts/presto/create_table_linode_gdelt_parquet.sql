CREATE SCHEMA IF NOT EXISTS hive.gdelt
WITH (location = 's3a://gdelt/');

CREATE TABLE IF NOT EXISTS hive.gdelt.gdelt_parquet_2020 (
  event_id        VARCHAR,
  date            DATE,
  event_date      DATE,
  event_code      BIGINT,
  event_base_code BIGINT,
  event_root_code BIGINT,
  lat             DOUBLE,
  lon             DOUBLE,
  geo_type        BIGINT,
  country_code    VARCHAR,
  adm1_code       VARCHAR,
  source_url      VARCHAR,
  netloc          VARCHAR
)
WITH (
  external_location = 's3a://gdelt/gdelt_parquet_2020', 
  format = 'PARQUET'
);

CREATE TABLE IF NOT EXISTS hive.gdelt.gdelt_parquet (
  event_id        VARCHAR,
  date            DATE,
  event_date      DATE,
  event_code      BIGINT,
  event_base_code BIGINT,
  event_root_code BIGINT,
  lat             DOUBLE,
  lon             DOUBLE,
  geo_type        BIGINT,
  country_code    VARCHAR,
  adm1_code       VARCHAR,
  source_url      VARCHAR,
  netloc          VARCHAR
)
WITH (
  external_location = 's3a://gdelt/gdelt_parquet', 
  format = 'PARQUET'
);

SHOW TABLES IN hive.gdelt;
