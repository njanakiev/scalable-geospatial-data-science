#CSV_FILEPATH="/home/hadoop/sgds/processed_data/2020_test_noheader_100k.csv"
#CSV_FILEPATH="/home/hadoop/sgds/processed_data/2019_filtered_noheader.csv"
CSV_FILEPATH="/home/hadoop/sgds/processed_data/20200101_filtered_noheader.csv"
FIELDS_FILEPATH="/home/hadoop/sgds/scripts/mongodb/mongodb_gdelt_fields.txt"
COLLECTION=gdelt_20200101

# --ignoreBlanks  Ignores empty fields in csv and tsv exports
# --drop  drops the collection before importing the data

# Reference date to parse dates:
# https://docs.mongodb.com/database-tools/mongoimport/#cmdoption-mongoimport-columnshavetypes
# column.date(2006-01-02T15:04:05.000Z)
# column.date(2006-01-02T15:04:05Z)
# column.date(2006-01-02)

mongoimport \
  --type csv \
  --ignoreBlanks \
  --username $MONGODB_USERNAME \
  --password $MONGODB_PASSWORD \
  --authenticationDatabase admin \
  --db sgds \
  --collection $COLLECTION \
  --columnsHaveTypes \
  --fieldFile $FIELDS_FILEPATH \
  --drop \
  --file=$CSV_FILEPATH

# 2020
# 24330238 document(s) imported successfully. 0 document(s) failed to import. 3.67GB
# real    45m28.693s
# user    17m2.235s
# sys     1m22.913s

# 2019
# 54815269 document(s) imported successfully. 0 document(s) failed to import. 8.14GB
# real    91m43.745s
# user    37m13.230s
# sys     3m6.665s


# real    172m7.550s
# user    0m0.431s
# sys     0m0.186s
time mongo \
  --username $MONGODB_USERNAME \
  --password $MONGODB_PASSWORD \
  --authenticationDatabase admin \
  mongodb/gdelt_2020_create_geometry_field.js
