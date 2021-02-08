set -e

#LOG_FILEPATH=presto_queries_344.log
#STATS_FILEPATH=presto_stats_344.csv
LOG_FILEPATH=presto_queries_0.242.log
STATS_FILEPATH=presto_stats_0.242.csv

rm -f $LOG_FILEPATH
echo "filepath,duration" > $STATS_FILEPATH

for filepath in query/*.sql; do
  echo "$filepath" | \
    tee --append $LOG_FILEPATH
  
  start=$(date +%s.%N)
  presto \
    --catalog hive \
    --schema default \
    --client-tags $filepath \
    -f $filepath | \
    tee --append $LOG_FILEPATH
  end=$(date +%s.%N)
  
  duration=$(echo "$end $start" | awk '{print $1-$2}')
  echo "Duration $duration" | \
    tee --append $LOG_FILEPATH
    
  echo "$filepath,$duration" >> $STATS_FILEPATH
done
