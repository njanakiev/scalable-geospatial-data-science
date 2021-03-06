EXPLAIN ANALYZE SELECT 
  event_date, 
  event_root_code, 
  lat,
  lon
FROM 
  ${GDELT_TABLE_NAME}
WHERE (event_date BETWEEN DATE '2010-01-01' AND DATE '2021-01-01')
  AND (-85 < lat) AND (lat < 85)
  AND ST_Intersects(
    ST_Point(lon, lat), 
    ST_GeometryFromText(
      'POLYGON ((17.1608 46.3723, 
                 17.1608 49.0205, 
                 9.5307 49.0205, 
                 9.5307 46.3723, 
                 17.1608 46.3723))'));
