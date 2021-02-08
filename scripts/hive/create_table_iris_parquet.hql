set parquet.compression=SNAPPY;

DROP TABLE IF EXISTS iris_parquet;

CREATE EXTERNAL TABLE iris_parquet (
  sepal_length DOUBLE,
  sepal_width  DOUBLE,
  petal_length DOUBLE,
  petal_width  DOUBLE,
  class        STRING
) 
STORED AS PARQUET
LOCATION 'hdfs://node-master:54310/user/hadoop/iris_parquet/iris.parq';

SELECT test_date FROM iris_parquet LIMIT 10;
