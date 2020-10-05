SELECT 
  gdelt."EventRootCode" AS eventcode,
  COUNT(gdelt."SQLDATE") AS counts,
  region.wkb_geometry AS geom
FROM gdelt_2020 as gdelt
JOIN states_provinces as region
ON ST_Contains(region.wkb_geometry, gdelt.geometry)
GROUP BY gdelt."EventRootCode", region.wkb_geometry
LIMIT 10;
