set -e

source .env

METASTORE_HOME=/usr/local/metastore

if [ -z "$HADOOP_HOME" ]; then
  echo "HADOOP_HOME evnironment variable not set"
  echo 'export HADOOP_HOME=/usr/local/hadoop' >> ~/.bashrc
  export HADOOP_HOME=/usr/local/hadoop
fi

if [ "$1" = "mariadb" ]; then
  # Install MariaDB
  sudo apt update
  sudo apt install -y mariadb-server
  sudo systemctl enable mariadb.service
  sudo systemctl start mariadb.service

  # Prepare user and database
  sudo mysql -u root -e "
    DROP DATABASE IF EXISTS metastore;
    CREATE DATABASE metastore;

    CREATE USER 'hive'@localhost IDENTIFIED BY 'hive';
    GRANT ALL PRIVILEGES ON *.* TO 'hive'@'localhost';
    FLUSH PRIVILEGES;"
fi

# Download Hive Standalone Metastore
wget "https://repo1.maven.org/maven2/org/apache/hive/hive-standalone-metastore/3.1.2/hive-standalone-metastore-3.1.2-bin.tar.gz"
tar -zxvf hive-standalone-metastore-3.1.2-bin.tar.gz
sudo mv apache-hive-metastore-3.1.2-bin $METASTORE_HOME
sudo chown $USER:$USER $METASTORE_HOME

# Download Hadoop
wget "https://downloads.apache.org/hadoop/common/hadoop-3.2.1/hadoop-3.2.1.tar.gz"
tar xvf hadoop-3.2.1.tar.gz
sudo mv hadoop-3.2.1 $HADOOP_HOME
sudo chown $USER:$USER $HADOOP_HOME

# Replace Guava library and add missing libraries
rm $METASTORE_HOME/lib/guava-19.0.jar
cp $HADOOP_HOME/share/hadoop/common/lib/guava-27.0-jre.jar $METASTORE_HOME/lib/
cp $HADOOP_HOME/share/hadoop/tools/lib/hadoop-aws-3.2.1.jar $METASTORE_HOME/lib/
cp $HADOOP_HOME/share/hadoop/tools/lib/aws-java-sdk-bundle-1.11.375.jar $METASTORE_HOME/lib/

# Download MySQL Connector
wget "https://repo1.maven.org/maven2/mysql/mysql-connector-java/8.0.20/mysql-connector-java-8.0.20.jar" \
  --directory-prefix=$METASTORE_HOME/lib/

envsubst < scripts/configuration/metastore-site.xml > \
  $METASTORE_HOME/conf/metastore-site.xml

$METASTORE_HOME/bin/schematool -initSchema -dbType mysql

# Start metastore with: $METASTORE_HOME/bin/start-metastore &
