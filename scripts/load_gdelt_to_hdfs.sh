hdfs dfs -rm -r gdelt_csv_2020
hdfs dfs -rm -r gdelt_csv_2019
hdfs dfs -rm -r gdelt_parquet_2020
hdfs dfs -rm -r gdelt_parquet_2019
hdfs dfs -rm -r gdelt_parquet_inserted_2020
hdfs dfs -rm -r gdelt_parquet_inserted_2019
hdfs dfs -rm -r /user/hive/warehouse

hdfs dfs -mkdir gdelt_csv_2020
hdfs dfs -mkdir gdelt_csv_2019
hdfs dfs -mkdir gdelt_parquet_2020
hdfs dfs -mkdir gdelt_parquet_2019
hdfs dfs -mkdir gdelt_parquet_inserted_2020
hdfs dfs -mkdir gdelt_parquet_inserted_2019
hdfs dfs -mkdir -p /user/hive/warehouse

# real    0m48.072s
# user    0m15.978s
# sys     0m11.658s
time hdfs dfs -put ../processed_data/2020_filtered_noheader.csv \
  gdelt_csv_2020/2020.csv

# real    2m41.239s
# user    0m26.677s
# sys     0m21.884s
time hdfs dfs -put ../processed_data/2019_filtered_noheader.csv \
  gdelt_csv_2019/2019.csv

# real    0m45.950s
# user    0m10.469s
# sys     0m3.968s
time hdfs dfs -put ../processed_data/2020_filtered.snappy.parq \
  gdelt_parquet_2020/2020.snappy.parq

# real    2m37.071s
# user    0m16.028s
# sys     0m7.411s
time hdfs dfs -put ../processed_data/2019_filtered.snappy.parq \
  gdelt_parquet_2019/2019.snappy.parq

#hive -f hive/create_table_gdelt_csv.hql
#hive -f hive/create_table_gdelt_parquet.hql

# real    2m34.696s
# user    0m44.079s
# sys     0m2.783s
hive -f hive/create_table_gdelt_parquet_inserted_2020.hql

# real    3m53.013s
# user    0m45.301s
# sys     0m3.031s
hive -f hive/create_table_gdelt_parquet_inserted_2019.hql
