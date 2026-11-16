#!/bin/bash
# Copies DAG, Python scripts and DDLs into the Airflow container.
set -e
cd "$(dirname "$0")/.."
sudo docker cp airflow/dags/mtg.py airflow:/home/airflow/airflow/dags/mtg.py
sudo docker cp airflow/python/. airflow:/home/airflow/airflow/python/
sudo docker cp ddl/. airflow:/home/airflow/airflow/ddl/
sudo docker exec airflow chown -R airflow:airflow /home/airflow/airflow/dags /home/airflow/airflow/python /home/airflow/airflow/ddl
# MySQL credentials for the Spark export job (without the root password), readable only by user airflow
grep -v '^MYSQL_ROOT_PASSWORD=' "$HOME/mtg.env" \
    | sudo docker exec -i -u airflow airflow bash -c 'umask 077; cat > /home/airflow/airflow/mtg_mysql.env'
echo "Deployed DAG, scripts and DDLs to Airflow."
