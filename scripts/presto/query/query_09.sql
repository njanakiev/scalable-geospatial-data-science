EXPLAIN ANALYZE WITH countries AS (
  SELECT 
    ne_id, 
    ST_GeomFromBinary(geometry) AS geometry
  FROM 
    ${NE_TABLE_NAME}
), points AS (
  SELECT 
    ST_Point(lon, lat) AS point 
  FROM 
    ${GDELT_TABLE_NAME}
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
