RUNTIME=/usr/local/trino/bin/trino

BUCKET="s3a://gdelt"
#BUCKET="s3a://gdelt-446756"
CATALOG=hive
SCHEMA=gdelt

command=$(cat << EOF
CREATE SCHEMA IF NOT EXISTS $CATALOG.$SCHEMA
WITH (location = '${BUCKET}/');

CREATE TABLE IF NOT EXISTS $CATALOG.$SCHEMA.gdelt_parquet_2020 (
  event_id        VARCHAR,
  date            DATE,
  event_date      DATE,
  event_code      BIGINT,
  event_base_code BIGINT,
  event_root_code BIGINT,
  lat             DOUBLE,
  lon             DOUBLE,
  geo_type        BIGINT,
  country_code    VARCHAR,
  adm1_code       VARCHAR,
  source_url      VARCHAR,
  netloc          VARCHAR
)
WITH (
  external_location = '${BUCKET}/gdelt_parquet_2020',
  format = 'PARQUET'
);

CREATE TABLE IF NOT EXISTS $CATALOG.$SCHEMA.gdelt_parquet (
  event_id        VARCHAR,
  date            DATE,
  event_date      DATE,
  event_code      BIGINT,
  event_base_code BIGINT,
  event_root_code BIGINT,
  lat             DOUBLE,
  lon             DOUBLE,
  geo_type        BIGINT,
  country_code    VARCHAR,
  adm1_code       VARCHAR,
  source_url      VARCHAR,
  netloc          VARCHAR
)
WITH (
  external_location = '${BUCKET}/gdelt_parquet',
  format = 'PARQUET'
);

CREATE TABLE IF NOT EXISTS $CATALOG.$SCHEMA.ne_110_countries_parquet (
  ne_id      BIGINT,
  name       VARCHAR,
  iso_a2     VARCHAR,
  geometry   VARBINARY
)
WITH (
  external_location = '${BUCKET}/ne_110_countries_parquet',
  format = 'PARQUET'
);

SHOW TABLES IN $CATALOG.$SCHEMA;
EOF
)

$RUNTIME --execute "$command"
