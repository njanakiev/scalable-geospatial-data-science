set parquet.compression=SNAPPY;

DROP TABLE IF EXISTS iris_csv;
CREATE EXTERNAL TABLE iris_csv (
  sepal_length DOUBLE,
  sepal_width  DOUBLE,
  petal_length DOUBLE,
  petal_width  DOUBLE,
  class        STRING,
  test_date    DATE
)  ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
LOCATION 'hdfs://node-master:54310/user/hadoop/iris_csv/';


DROP TABLE IF EXISTS iris_parquet_inserted;
CREATE EXTERNAL TABLE iris_parquet_inserted (
  sepal_length DOUBLE,
  sepal_width  DOUBLE,
  petal_length DOUBLE,
  petal_width  DOUBLE,
  class        STRING,
  test_date    DATE
) 
STORED AS PARQUET
LOCATION 'hdfs://node-master:54310/user/hadoop/iris_parquet_inserted';

INSERT INTO iris_parquet_inserted
SELECT * FROM iris_csv;
