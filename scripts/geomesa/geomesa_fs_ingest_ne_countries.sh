HDFS_HOST="hdfs://node-master:54310"
HDFS_FILEPATH="/tmp/geomesa/ne/countries"
CONF_FILEPATH="geomesa_ne_countries.conf"
DATA_FILEPATH="/home/hadoop/sgds/processed_data/ne_110m_admin_0_countries.csv"

hdfs dfs -rm -r $HDFS_FILEPATH

/usr/local/geomesa-fs/bin/geomesa-fs ingest \
  --encoding parquet \
  --partition-scheme xz2-10bit \
  --path "${HDFS_HOST}${HDFS_FILEPATH}" \
  --converter $CONF_FILEPATH \
  --spec $CONF_FILEPATH \
  --num-reducers 2 \
  $DATA_FILEPATH
