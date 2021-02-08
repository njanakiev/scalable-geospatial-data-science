EXPLAIN ANALYZE SELECT 
  event_root_code,
  COUNT(event_id) AS cnt
FROM 
  hive.default.gdelt_parquet
GROUP BY 
  event_root_code
ORDER BY 2 DESC;
