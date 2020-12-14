set -e

LOG_FILEPATH=presto_queries.log

rm $LOG_FILEPATH

for filepath in query/*.sql; do
  echo "$filepath" | \
    tee --append $LOG_FILEPATH
  time presto \
    --catalog hive \
    --schema default \
    --client-tags $filepath \
    -f $filepath | \
    tee --append $LOG_FILEPATH
done
