CREATE SCHEMA IF NOT EXISTS hive.gdelt
WITH (location = 's3a://gdelt/');

CREATE TABLE IF NOT EXISTS hive.gdelt.ne_110_countries_parquet (
  ne_id      BIGINT,
  name       VARCHAR,
  iso_a2     VARCHAR,
  geometry   VARBINARY
)
WITH (
  external_location = 's3a://gdelt/ne_110_countries_parquet',
  format = 'PARQUET'
);

SHOW TABLES IN hive.gdelt;
