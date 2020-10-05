# real    33m52.894s
# user    26m2.721s
# sys     4m0.637s
# 16083528 ingested 8926313 failed in 00:33:01

HDFS_HOST="hdfs://node-master:54310"
#HDFS_FILEPATH="/tmp/geomesa/gdelt_custom"
HDFS_FILEPATH="/tmp/geomesa/gdelt_custom_2020"
CONF_FILEPATH="geomesa_gdelt_custom.conf"
#DATA_FILEPATH="/home/hadoop/sgds/processed_data/2020_test.csv"
#DATA_FILEPATH="/home/hadoop/sgds/processed_data/2020_test_noheader_100k.csv"
DATA_FILEPATH="/home/hadoop/sgds/processed_data/2020_filtered_noheader.csv"

hdfs dfs -rm -r $HDFS_FILEPATH

time /usr/local/geomesa-fs/bin/geomesa-fs ingest \
  --encoding parquet \
  --partition-scheme z2-8bit \
  --path "${HDFS_HOST}${HDFS_FILEPATH}" \
  --converter $CONF_FILEPATH \
  --spec $CONF_FILEPATH \
  --num-reducers 2 \
  $DATA_FILEPATH

# 2020
# 100% complete 24330238 ingested 0 failed in 00:20:15
# INFO  Local ingestion complete in 00:20:38
# INFO  Ingested 24330238 features with no failures for file: /home/hadoop/sgds/processed_data/2020_filtered_noheader.csv
# real    20m43.756s
# user    21m59.054s
# sys     1m23.437s
