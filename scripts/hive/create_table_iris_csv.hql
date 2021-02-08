DROP TABLE IF EXISTS tutorial.iris;

CREATE TABLE tutorial.iris (
  sepal_length FLOAT,
  sepal_width  FLOAT,
  petal_length FLOAT,
  petal_width  FLOAT,
  class        STRING
) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';

LOAD DATA LOCAL INPATH '/home/hadoop/sgds/data/iris.data' 
OVERWRITE INTO TABLE tutorial.iris;
