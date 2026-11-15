#!/bin/bash
# One-time setup on the VM (run from the mtg_exam directory, after the
# containers "hadoop" and "airflow" from the lecture are running):
#   - MySQL container (end-user database) in network bigdatanet
#   - MySQL credentials (generated once, kept in ~/mtg.env on the VM)
#   - Airflow: pymysql, MySQL JDBC driver, connection "mtg_mysql"
#   - Frontend container on port 5000
set -e
cd "$(dirname "$0")/.."

ENV_FILE=$HOME/mtg.env
if [ ! -f "$ENV_FILE" ]; then
    cat > "$ENV_FILE" <<ENV
MYSQL_HOST=mysql
MYSQL_PORT=3306
MYSQL_DATABASE=mtg
MYSQL_USER=mtg
MYSQL_PASSWORD=$(openssl rand -hex 12)
MYSQL_ROOT_PASSWORD=$(openssl rand -hex 12)
ENV
    chmod 600 "$ENV_FILE"
fi
. "$ENV_FILE"

# MySQL
if ! sudo docker ps -a --format '{{.Names}}' | grep -qx mysql; then
    sudo docker run -d --name mysql --net bigdatanet --restart unless-stopped \
        --env-file "$ENV_FILE" -v mtg_mysql_data:/var/lib/mysql \
        mysql:8.0 --character-set-server=utf8mb4 --collation-server=utf8mb4_unicode_ci \
        --default-authentication-plugin=mysql_native_password
fi

# Airflow
sudo docker exec -u airflow airflow bash -lc "pip3 install --user -q 'pymysql==1.0.2'; mkdir -p /home/airflow/airflow/lib /home/airflow/airflow/ddl /home/airflow/airflow/python"
sudo docker exec -u airflow airflow bash -lc "cd /home/airflow/airflow/lib && [ -f mysql-connector-java-8.0.28.jar ] || wget -q https://repo1.maven.org/maven2/mysql/mysql-connector-java/8.0.28/mysql-connector-java-8.0.28.jar"
sudo docker exec -u airflow airflow bash -lc "airflow connections --delete --conn_id mtg_mysql >/dev/null 2>&1; airflow connections --add --conn_id mtg_mysql --conn_type mysql --conn_host $MYSQL_HOST --conn_port $MYSQL_PORT --conn_schema $MYSQL_DATABASE --conn_login $MYSQL_USER --conn_password $MYSQL_PASSWORD >/dev/null && echo 'Airflow connection mtg_mysql set'"

# Frontend
sudo docker build -q -t mtg-frontend frontend
sudo docker rm -f mtg-frontend >/dev/null 2>&1 || true
sudo docker run -d --name mtg-frontend --net bigdatanet --restart unless-stopped \
    --env-file "$ENV_FILE" -p 5000:5000 mtg-frontend

./setup/deploy.sh
echo "Setup finished."
