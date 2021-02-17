# Install MariaDB
sudo apt update
sudo apt install -y mariadb-server

sudo systemctl enable mariadb.service
sudo systemctl start mariadb.service

# Prepare user and database
sudo mysql -u root -e "
DROP DATABASE IF EXISTS 'metastore'; 
CREATE DATABASE 'metastore';

CREATE USER 'hive'@localhost IDENTIFIED BY 'hive';
GRANT ALL PRIVILEGES ON *.* TO 'hive'@'localhost';
FLUSH PRIVILEGES;"
