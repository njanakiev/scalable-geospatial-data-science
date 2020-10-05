#VERSION="1.2.2"
#VERSION="2.3.7"
VERSION="3.1.2"
FILEPATH="/home/hadoop/apache-hive-${VERSION}-bin.tar.gz"

#wget "https://downloads.apache.org/hive/hive-${VERSION}/apache-hive-${VERSION}-bin.tar.gz" \
#  -O $FILEPATH

#sudo tar xvf $FILEPATH \
#  --directory="/usr/local/hive" \
#  --strip 1

# Change ownership to hadoop user
sudo chown -R hadoop:hadoop "/usr/local/hive"

# Add configuration
cp "configuration/hive-site.xml" \
   "/usr/local/hive/conf/" 

# Download MySQL Connector
wget "https://repo1.maven.org/maven2/mysql/mysql-connector-java/8.0.20/mysql-connector-java-8.0.20.jar" \
  --directory-prefix="/usr/local/hive/lib/"

rm "/usr/local/hive/lib/guava-"*
cp "/usr/local/hadoop/share/hadoop/hdfs/lib/guava-"* \
   "/usr/local/hive/lib"

# Drop metastore database
#mysql -u hive -D metastore -e "DROP DATABASE metastore;" -p
