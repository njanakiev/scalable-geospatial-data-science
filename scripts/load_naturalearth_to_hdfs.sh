hdfs dfs -rm -r ne_parquet
hdfs dfs -mkdir ne_parquet
#time hdfs dfs -put ../processed_data/ne_110_countries.snappy.parq \
#  ne_parquet/ne_110_countries.snappy.parq

time hdfs dfs -put ../processed_data/ne_10_states_provinces.snappy.parq \
  ne_parquet/ne_10_states_provinces.snappy.parq

#hive -f ../sql-scripts/create_table_ne_110_countries.hql
#hive -f ../sql-scripts/create_table_ne_10_states_provinces.hql
