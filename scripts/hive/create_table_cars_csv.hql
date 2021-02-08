DROP TABLE IF EXISTS tutorial.cars;

CREATE TABLE tutorial.cars (
  buying   STRING,
  maint    STRING,
  doors    INT,
  persons  INT,
  lug_boot STRING,
  safety   STRING,
  class    STRING 
) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';

LOAD DATA LOCAL INPATH '/home/hadoop/sgds/data/cars.data' 
OVERWRITE INTO TABLE tutorial.cars;
