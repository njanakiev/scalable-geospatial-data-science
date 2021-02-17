EXPLAIN ANALYZE SELECT 
  points.event_root_code,
  COUNT(points.event_id) AS cnt
FROM 
  ${GDELT_TABLE_NAME} AS points,
  ${NE_TABLE_NAME} AS countries
WHERE 
  countries.iso_a2 = 'AT'
  AND ST_Contains(ST_GeomFromBinary(countries.geometry), 
                  ST_Point(points.lon, points.lat))
GROUP BY
  points.event_root_code;
