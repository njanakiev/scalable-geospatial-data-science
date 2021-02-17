EXPLAIN ANALYZE SELECT 
  event_root_code,
  COUNT(event_id) AS cnt
FROM 
  ${GDELT_TABLE_NAME}
GROUP BY 
  event_root_code
ORDER BY 2 DESC;
