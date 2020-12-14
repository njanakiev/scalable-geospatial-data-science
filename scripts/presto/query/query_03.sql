EXPLAIN ANALYZE SELECT
  event_root_code,
  date_trunc('month', event_date) AS event_date,
  COUNT(event_id) AS cnt
FROM
  gdelt_parquet_inserted_2020
GROUP BY (
  event_root_code,
  date_trunc('month', event_date));
