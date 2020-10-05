/usr/local/geomesa-fs/bin/geomesa-fs ingest \
  --encoding parquet \
  --partition-scheme daily,z2-2bit \
  --path hdfs://node-master:54310/tmp/geomesa/1 \
  --converter gdelt \
  --spec gdelt \
  --num-reducers 10 \
  /home/hadoop/sgds/data/raw/20200101.export.csv
