EXPLAIN ANALYZE WITH countries AS (
  SELECT 
    ne_id, 
    ST_GeomFromBinary(geometry) AS geometry
  FROM 
    hive.default.ne_110_countries_parquet
), points AS (
  SELECT 
    ST_Point(lon, lat) AS point 
  FROM 
    hive.default.gdelt_parquet
)

SELECT 
  countries.ne_id,
  COUNT(*) AS cnt
FROM 
  points, 
  countries
WHERE 
  ST_Contains(countries.geometry, points.point)
GROUP BY
  countries.ne_id;
