EXPLAIN ANALYZE SELECT 
  points.event_root_code,
  COUNT(points.event_id) AS cnt
FROM 
  gdelt_parquet_inserted_2020 AS points,
  ne_110_countries_parquet AS countries
WHERE 
  countries.iso_a2 = 'AT'
  AND ST_Contains(ST_GeomFromBinary(countries.geometry), 
                  ST_Point(points.lon, points.lat))
GROUP BY
  points.event_root_code;
