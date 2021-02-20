EXPLAIN ANALYZE SELECT 
  points.event_date,
  points.event_root_code,
  points.lat,
  points.lon
FROM 
  ${GDELT_TABLE_NAME} AS points,
  ${NE_TABLE_NAME} AS countries
WHERE 
  countries.iso_a2 = 'AT'
  AND (-85 < points.lat) AND (points.lat < 85)
  AND ST_Contains(ST_GeomFromBinary(countries.geometry), 
                  ST_Point(points.lon, points.lat))
LIMIT 1000;
