INDEX=ne_country

# Delete index
curl \
  -u $ES_USERNAME:$ES_PASSWORD \
  -X DELETE "${ES_HOST}:${ES_PORT}/${INDEX}?pretty"

# Create mapping for the data
curl \
  -u $ES_USERNAME:$ES_PASSWORD \
  -X PUT "${ES_HOST}:${ES_PORT}/_template/${INDEX}?pretty" \
  -H 'Content-Type: application/json' \
  -d'
{
    "order": 10,
    "index_patterns": [
        "'"${INDEX}"'*"
    ],
    "settings": {
        "index": {
            "number_of_shards": "1",
            "number_of_replicas": "0"
        }
    },
    "mappings": {
        "properties": {
            "geometry": {
                "type": "geo_shape"
            }
        }
    },
    "aliases": {}
}'

# Load data
cat /home/hadoop/sgds/processed_data/ne_110m_admin_0_countries.csv | \
  /usr/share/logstash/bin/logstash -f \
  /home/hadoop/sgds/scripts/logstash_ingest_ne_countries.conf
