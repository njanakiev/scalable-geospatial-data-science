EXPLAIN ANALYZE SELECT 
  points.event_date,
  points.event_root_code,
  points.lat,
  points.lon
FROM 
  gdelt_parquet_inserted_2020 AS points,
  ne_110_countries_parquet AS countries
WHERE 
  countries.iso_a2 = 'AT'
  AND ST_Contains(ST_GeomFromBinary(countries.geometry), 
                  ST_Point(points.lon, points.lat))
LIMIT 1000;
