CONF_FILEPATH="geomesa_ne_countries.conf"
#DATA_FILEPATH="/home/hadoop/sgds/processed_data/2020_test.csv"
#DATA_FILEPATH="/home/hadoop/sgds/processed_data/2020_test_noheader_100k.csv"
#DATA_FILEPATH="/home/hadoop/sgds/processed_data/2020_filtered_noheader.csv"
DATA_FILEPATH="/home/hadoop/sgds/processed_data/ne_110m_admin_0_countries.csv"
CATALOG_NAME=ne_countries

time /usr/local/geomesa-hbase_2.11-3.0.0/bin/geomesa-hbase ingest \
  --catalog $CATALOG_NAME \
  --converter $CONF_FILEPATH \
  --spec $CONF_FILEPATH \
  $DATA_FILEPATH