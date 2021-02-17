#VERSION="2.10.0"
#VERSION="3.1.3"
VERSION="3.2.1"
FILEPATH="/home/hadoop/hadoop-${VERSION}.tar.gz"

wget "https://downloads.apache.org/hadoop/common/hadoop-${VERSION}/hadoop-${VERSION}.tar.gz" \
  -O $FILEPATH

sudo tar xvf $FILEPATH \
  --directory="/usr/local/hadoop" \
  --strip 1
