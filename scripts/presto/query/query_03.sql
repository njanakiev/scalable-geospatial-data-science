EXPLAIN ANALYZE SELECT
  event_root_code,
  date_trunc('month', event_date) AS event_date,
  COUNT(event_id) AS cnt
FROM
  ${GDELT_TABLE_NAME}
GROUP BY (
  event_root_code,
  date_trunc('month', event_date))
ORDER BY 3 DESC;
