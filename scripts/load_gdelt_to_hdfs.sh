hdfs dfs -rm -r gdelt_csv_2020
hdfs dfs -rm -r gdelt_parquet_2020
hdfs dfs -rm -r gdelt_parquet
hdfs dfs -rm -r /user/hive/warehouse

hdfs dfs -mkdir gdelt_csv_2020
hdfs dfs -mkdir -p /user/hive/warehouse

time hdfs dfs -put ../processed_data/2020_filtered_noheader.csv \
  gdelt_csv_2020/2020.csv

time hdfs dfs -put ../processed_data/gdelt_2020_500MB.snappy.parq \
  gdelt_2020_500MB.snappy.parq

time hdfs dfs -put ../processed_data/gdelt_500MB.snappy.parq \
  gdelt_500MB.snappy.parq

hive -f hive/create_table_gdelt_csv.hql
hive -f hive/create_table_gdelt_parquet.hql
