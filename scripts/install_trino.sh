set -e

source .env

TRINO_HOME=/usr/local/trino
CORDINATOR=true

sudo apt-get update
sudo apt-get install -y 
  openjdk-11-jdk-headless \
  openjdk-11-jre-headless \
  openjdk-11-jre \
  python-is-python3 \
  uuid

wget "https://repo1.maven.org/maven2/io/trino/trino-server/352/trino-server-352.tar.gz"
tar -xzvf trino-server-352.tar.gz
sudo mv trino-server-352 $TRINO_HOME
sudo chown $USER:$USER $TRINO_HOME

if [ "$1" = "cli" ]; then
  wget "https://repo1.maven.org/maven2/io/trino/trino-cli/352/trino-cli-352-executable.jar"
  mv trino-cli-352-executable.jar $TRINO_HOME/bin/trino
  sudo chmod +x $TRINO_HOME/bin/trino
fi

# Trino Configuration
mkdir -p $TRINO_HOME/etc/catalog

cat > $TRINO_HOME/etc/jvm.config << EOF
-server
-Xmx6G
-XX:+UseG1GC
-XX:G1HeapRegionSize=32M
-XX:+UseGCOverheadLimit
-XX:+ExplicitGCInvokesConcurrent
-XX:+HeapDumpOnOutOfMemoryError
-XX:+ExitOnOutOfMemoryError
-Djdk.attach.allowAttachSelf=true
EOF

NODE_UUID=$(uuid -v 1)
cat > $TRINO_HOME/etc/node.properties << EOF
node.environment=production
node.id=$NODE_UUID
node.data-dir=$TRINO_HOME/data
EOF

cat > $TRINO_HOME/etc/config.properties << EOF
coordinator=$CORDINATOR
node-scheduler.include-coordinator=$CORDINATOR
http-server.http.port=8080
query.max-memory=50GB
query.max-memory-per-node=1GB
query.max-total-memory-per-node=2GB
discovery-server.enabled=true
discovery.uri=http://localhost:8080
EOF

cat > $TRINO_HOME/etc/catalog/hive.properties << EOF
connector.name=hive-hadoop2
hive.metastore.uri=thrift://localhost:9083
hive.s3.path-style-access=true
hive.s3.endpoint=$S3_ENDPOINT
hive.s3.aws-access-key=$S3_ACCESS_KEY
hive.s3.aws-secret-key=$S3_SECRET_KEY
hive.s3.ssl.enabled=false
EOF
