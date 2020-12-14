EXPLAIN ANALYZE SELECT 
  event_date, 
  event_root_code, 
  lat,
  lon
FROM gdelt_parquet_inserted_2020
WHERE ST_Intersects(
  ST_Point(lon, lat), 
  ST_GeometryFromText(
      'POLYGON ((17.1608 46.3723, 
                 17.1608 49.0205, 
                 9.5307 49.0205, 
                 9.5307 46.3723, 
                 7.1608 46.3723))'));
