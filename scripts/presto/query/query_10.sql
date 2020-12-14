EXPLAIN ANALYZE WITH countries AS (
  SELECT 
    ne_id,
    iso_a2,
    ST_GeomFromBinary(geometry) AS geometry
  FROM 
    ne_110_countries_parquet
), points AS (
   SELECT
     event_root_code, 
     ST_Point(lon, lat) AS point
   FROM 
    gdelt_parquet_inserted_2020
)

SELECT 
  countries.ne_id,
  countries.iso_a2,
  points.event_root_code,
  COUNT(*) AS cnt
FROM 
  points, 
  countries
WHERE 
  ST_Contains(countries.geometry, points.point)
GROUP BY (
  countries.ne_id, 
  countries.iso_a2,
  points.event_root_code
);
