EXPLAIN ANALYZE SELECT 
  COUNT(event_id) AS cnt
FROM 
  hive.default.gdelt_parquet;
