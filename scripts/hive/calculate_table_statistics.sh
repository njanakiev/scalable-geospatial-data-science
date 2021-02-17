hive -v -e "
-- Enable snappy compression for parquet files
SET parquet.compression=SNAPPY;

-- SET mapreduce.map.memory.mb=4096;
SET mapreduce.map.java.opts=-Xmx2048m;
-- SET mapreduce.reduce.memory.mb=4096;
-- SET mapreduce.reduce.java.opts=-Xmx3686m;

-- ANALYZE TABLE gdelt_parquet_2020 COMPUTE STATISTICS FOR COLUMNS;
-- ANALYZE TABLE gdelt_parquet COMPUTE STATISTICS FOR COLUMNS;
ANALYZE TABLE ne_110_countries_parquet COMPUTE STATISTICS FOR COLUMNS;
ANALYZE TABLE ne_10_states_provinces_parquet COMPUTE STATISTICS FOR COLUMNS;
-- ANALYZE TABLE iris_parquet COMPUTE STATISTICS FOR COLUMNS;"
