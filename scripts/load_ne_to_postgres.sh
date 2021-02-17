#FILEPATH="naturalearth/ne_10m_admin_1_states_provinces.shp"
FILEPATH="naturalearth/ne_10m_admin_0_countries.shp"
#TABLE_NAME=states_provinces
TABLE_NAME=countries

ogr2ogr -f "PostgreSQL" \
  PG:"dbname='sgds' \
      host='${POSTGRES_HOST}' \
      port='${POSTGRES_PORT}' \
      user='${POSTGRES_USERNAME}' \
      password='${POSTGRES_PASSWORD}'" \
  $FILEPATH \
  -nln $TABLE_NAME \
  -nlt MULTIPOLYGON
