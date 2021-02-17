hdfs dfs -rm -r ne_countries_parquet
hdfs dfs -rm -r ne_states_provinces_parquet
hdfs dfs -mkdir ne_countries_parquet
hdfs dfs -mkdir ne_states_provinces_parquet

time hdfs dfs -put \
  ../processed_data/ne_110_countries.snappy.parq \
  ne_countries_parquet/ne_110_countries.snappy.parq

time hdfs dfs -put \
  ../processed_data/ne_10_states_provinces.snappy.parq \
  ne_states_provinces_parquet/ne_10_states_provinces.snappy.parq

hive -f hive/create_table_ne_parquet.hql
