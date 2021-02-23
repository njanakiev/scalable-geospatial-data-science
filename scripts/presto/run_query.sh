set -e

LOG_FOLDERPATH=log

mkdir -p $LOG_FOLDERPATH

#LOG_FILEPATH=$LOG_FOLDERPATH/presto_queries_344.log
#STATS_FILEPATH=$LOG_FOLDERPATH/presto_stats_344.csv
#LOG_FILEPATH=$LOG_FOLDERPATH/presto_queries_0.242_s.log
#STATS_FILEPATH=$LOG_FOLDERPATH/presto_stats_0.242_s.csv
#LOG_FILEPATH=$LOG_FOLDERPATH/presto_queries_0.247_s.log
#STATS_FILEPATH=$LOG_FOLDERPATH/presto_stats_0.247_s.csv
LOG_FILEPATH=$LOG_FOLDERPATH/presto_queries_minio2_352_cpx51.log
STATS_FILEPATH=$LOG_FOLDERPATH/presto_stats_minio2_352_cpx51.csv

RUNTIME=/usr/local/trino/bin/trino
CATALOG=hive
SCHEMA=gdelt

export GDELT_TABLE_NAME=$CATALOG.$SCHEMA.gdelt_parquet
export NE_TABLE_NAME=$CATALOG.$SCHEMA.ne_110_countries_parquet

# Run warmup queries
time $RUNTIME \
  --catalog $CATALOG \
  --schema $SCHEMA \
  --execute "
    SELECT * FROM gdelt_parquet LIMIT 100;
    SELECT * FROM gdelt_parquet_2020 LIMIT 100;
    SELECT * FROM ne_110_countries_parquet LIMIT 100;" \
    > /dev/null

rm -f $LOG_FILEPATH
echo "filepath,duration" > $STATS_FILEPATH

for filepath in query/*.sql; do
  echo "$filepath" | \
    tee --append $LOG_FILEPATH

  query=$(envsubst < $filepath)

  start=$(date +%s.%N)
  $RUNTIME \
    --execute "${query}" \
    --client-tags $filepath | \
    tee --append $LOG_FILEPATH
  end=$(date +%s.%N)

  duration=$(echo "$end $start" | awk '{print $1-$2}')
  echo "Duration $duration" | \
    tee --append $LOG_FILEPATH

  echo "$filepath,$duration" >> $STATS_FILEPATH
done
