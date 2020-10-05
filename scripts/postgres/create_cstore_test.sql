DROP TABLE IF EXISTS cstore_points_test;

CREATE FOREIGN TABLE cstore_points_test ( 
    SQLDATE TEXT, 
    ActionGeo_Lat FLOAT,
    ActionGeo_Long FLOAT,
    SOURCEURL TEXT
SERVER cstore_server OPTIONS(compression 'pglz');

COPY cstore_points_test
FROM '/home/hadoop/sgds/processed_data/2020_test.csv'
WITH CSV;
