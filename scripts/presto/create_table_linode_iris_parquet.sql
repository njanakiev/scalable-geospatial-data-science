CREATE SCHEMA IF NOT EXISTS hive.iris
WITH (location = 's3a://gdelt/');

CREATE TABLE IF NOT EXISTS hive.iris.iris_parquet (
  sepal_length DOUBLE,
  sepal_width  DOUBLE,
  petal_length DOUBLE,
  petal_width  DOUBLE,
  class        VARCHAR
)
WITH (
  external_location = 's3a://gdelt/iris_parquet',
  format = 'PARQUET'
);

SHOW TABLES IN hive.iris;
