INDEX=gdelt_custom_2020

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
            "number_of_shards": "5",
            "number_of_replicas": "1"
        }
    },
    "mappings": {
        "properties": {
            "location": {
                "type": "geo_point"
            }
        }
    },
    "aliases": {}
}'

# Load data
#cat /home/hadoop/sgds/processed_data/2020_test_noheader_100k.csv | \
#  /usr/share/logstash/bin/logstash -f \
#  /home/hadoop/sgds/scripts/logstash_ingest_gdelt.conf

#cat /home/hadoop/sgds/processed_data/20200101_filtered_noheader.csv | \
#  /usr/share/logstash/bin/logstash -f \
#  /home/hadoop/sgds/scripts/elasticsearch/logstash_ingest_gdelt.conf

# real    95m0.294s
# user    77m19.002s
# sys     2m8.739s
cat /home/hadoop/sgds/processed_data/2020_filtered_noheader.csv | \
  /usr/share/logstash/bin/logstash -f \
  /home/hadoop/sgds/scripts/elasticsearch/logstash_ingest_gdelt_2020.conf
