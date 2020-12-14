EXPLAIN ANALYZE SELECT 
  event_root_code,
  COUNT(event_id) AS cnt
FROM 
  gdelt_parquet_inserted_2020
GROUP BY 
  event_root_code;
