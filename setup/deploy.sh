#!/bin/bash
# Copies DAG, Python scripts and DDLs into the Airflow container.
set -e
cd "$(dirname "$0")/.."
sudo docker cp airflow/dags/mtg.py airflow:/home/airflow/airflow/dags/mtg.py
sudo docker cp airflow/python/. airflow:/home/airflow/airflow/python/
sudo docker cp ddl/. airflow:/home/airflow/airflow/ddl/
sudo docker exec airflow chown -R airflow:airflow /home/airflow/airflow/dags /home/airflow/airflow/python /home/airflow/airflow/ddl
echo "Deployed DAG, scripts and DDLs to Airflow."
