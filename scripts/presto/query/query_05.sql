EXPLAIN ANALYZE SELECT
  event_root_code,
  COUNT(event_id) AS cnt
FROM 
  hive.default.gdelt_parquet_inserted_2020
WHERE 
  ST_Distance(to_spherical_geography(ST_Point(2.349014, 48.864716)),
              to_spherical_geography(ST_Point(lon, lat))) < 10000
GROUP BY 
  event_root_code;
