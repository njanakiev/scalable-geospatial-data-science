set -e

#LOG_FILEPATH=log/presto_queries_344.log
#STATS_FILEPATH=log/presto_stats_344.csv
#LOG_FILEPATH=log/presto_queries_0.242_s.log
#STATS_FILEPATH=log/presto_stats_0.242_s.csv
LOG_FILEPATH=log/presto_queries_0.247_s.log
STATS_FILEPATH=log/presto_stats_0.247_s.csv
RUNTIME=presto

export GDELT_TABLE_NAME=hive.default.gdelt_parquet
export NE_TABLE_NAME=hive.default.ne_110_countries_parquet

# Run warmup queries
time $RUNTIME \
  --file warmup.sql \
  --catalog hive \
  --schema default > /dev/null

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
