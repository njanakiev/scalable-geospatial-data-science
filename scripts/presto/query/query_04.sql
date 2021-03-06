EXPLAIN ANALYZE SELECT
  event_date,
  event_root_code,
  lat,
  lon,
  ST_Distance(to_spherical_geography(ST_Point(2.349014, 48.864716)),
              to_spherical_geography(ST_Point(lon, lat))) AS distance
FROM 
  ${GDELT_TABLE_NAME}
WHERE (-85 < lat) AND (lat < 85)
ORDER BY 5 ASC
LIMIT 100;
