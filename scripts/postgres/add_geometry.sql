ALTER TABLE gdelt_2020_test 
ADD COLUMN geom geometry(Point,4326);

UPDATE gdelt_2020_test 
SET geom = ST_SetSRID(ST_MakePoint(
    CAST(ActionGeo_Long AS FLOAT), 
    CAST(ActionGeo_Lat AS FLOAT)), 4326);
