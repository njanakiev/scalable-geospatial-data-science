EXPLAIN ANALYZE SELECT 
  COUNT(event_id) AS cnt
FROM 
  ${GDELT_TABLE_NAME};
