# 2020
# 100% complete 24330238 ingested 0 failed in 00:53:23
# INFO  Local ingestion complete in 00:53:25
# INFO  Ingested 24330238 features with no failures for file: /home/hadoop/sgds/processed_data/2020_filtered_noheader.csv
# real    40m8.945s
# user    24m53.436s
# sys     1m6.064s


CONF_FILEPATH="geomesa_gdelt_custom.conf"
#DATA_FILEPATH="/home/hadoop/sgds/processed_data/2020_test.csv"
#DATA_FILEPATH="/home/hadoop/sgds/processed_data/2020_test_noheader_100k.csv"
#DATA_FILEPATH="/home/hadoop/sgds/processed_data/2020_filtered_noheader.csv"
DATA_FILEPATH="/home/hadoop/sgds/processed_data/2020_filtered_noheader.csv"
CATALOG_NAME=gdelt_custom_2020

time /usr/local/geomesa-hbase_2.11-3.0.0/bin/geomesa-hbase ingest \
  --catalog $CATALOG_NAME \
  --converter $CONF_FILEPATH \
  --spec $CONF_FILEPATH \
  $DATA_FILEPATH
