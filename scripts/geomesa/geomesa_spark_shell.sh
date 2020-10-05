#VERSION=2.4.1
VERSION=3.0.0

GEOMESA_FS_SPARK_RUNTIME=$GEOMESA_FS_HOME/dist/spark/geomesa-fs-spark-runtime_2.11-$VERSION.jar
GEOMESA_HBASE_SPARK_RUNTIME=$GEOMESA_HBASE_HOME/dist/spark/geomesa-hbase-spark-runtime-hbase2_2.11-$VERSION.jar

spark-shell \
  --driver-java-options "-Dhive.metastore.uris=thrift://node-master:9083" \
  --jars $GEOMESA_HBASE_SPARK_RUNTIME,$GEOMESA_FS_SPARK_RUNTIME \
  --master yarn \
  --driver-memory 8g
