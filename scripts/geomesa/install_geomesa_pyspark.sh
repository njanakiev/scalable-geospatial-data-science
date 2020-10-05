VERSION=3.0.0

mvn clean install -Ppython
pip install geomesa-spark/geomesa_pyspark/target/geomesa_pyspark-$VERSION.tar.gz
