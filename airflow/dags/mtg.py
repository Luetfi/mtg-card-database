# -*- coding: utf-8 -*-

"""
Title: MTG Trading Card Database By API
Description:
Practical Exam Big Data (DHBW Stuttgart).
Downloads all Magic The Gathering cards from api.magicthegathering.io,
stores the raw JSON in HDFS, transfers it to a cleaned final layer (Parquet),
and exports it to the end-user database (MySQL) used by the search frontend.
"""

import re
from datetime import datetime

from airflow import DAG
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from airflow.hooks.base_hook import BaseHook
from airflow.models import Variable
from airflow.operators.bash_operator import BashOperator
from airflow.operators.filesystem_operations import ClearDirectoryOperator
from airflow.operators.filesystem_operations import CreateDirectoryOperator
from airflow.operators.hdfs_operations import HdfsMkdirFileOperator
from airflow.operators.hive_operator import HiveOperator
from airflow.operators.python_operator import PythonOperator

AIRFLOW_HOME = '/home/airflow/airflow'
LOCAL_IMPORT_DIR = '/home/airflow/mtg'
HDFS_RAW_DIR = '/user/hadoop/mtg/raw'
HDFS_FINAL_DIR = '/user/hadoop/mtg/final'
MYSQL_CONN_ID = 'mtg_mysql'
MYSQL_JDBC_JAR = AIRFLOW_HOME + '/lib/mysql-connector-java-8.0.28.jar'
MYSQL_TABLES = ['cards', 'foreign_names']
STAGING_SUFFIX = '_staging'
# MySQL credentials for the Spark job (written by setup/deploy.sh from ~/mtg.env).
# Passed as file, so the password is not visible on the command line or in the Airflow UI.
MYSQL_ENV_FILE = AIRFLOW_HOME + '/mtg_mysql.env'
# A full run may shrink the card table by at most 10 % (protects against incomplete API responses)
MIN_RATIO_FULL_RUN = 0.9
# Raw partitions (days) that stay in HDFS
KEEP_RAW_DAYS = 3

# Raw data is partitioned by load date: /user/hadoop/mtg/raw/YYYY/MM/DD
YEAR = '{{ macros.ds_format(ds, "%Y-%m-%d", "%Y") }}'
MONTH = '{{ macros.ds_format(ds, "%Y-%m-%d", "%m") }}'
DAY = '{{ macros.ds_format(ds, "%Y-%m-%d", "%d") }}'
HDFS_RAW_PARTITION_DIR = HDFS_RAW_DIR + '/' + YEAR + '/' + MONTH + '/' + DAY

# Airflow Variable "mtg_max_pages": 0 = download all pages (default),
# any other number limits the download (e.g. for a quick test run).
MAX_PAGES = Variable.get('mtg_max_pages', default_var='0')


def read_ddl(layer, table):
    with open('%s/ddl/%s/%s.sql' % (AIRFLOW_HOME, layer, table)) as ddl_file:
        return ddl_file.read()


def mysql_connect():
    """Connects with the credentials of the Airflow connection mtg_mysql (no passwords in code)."""
    import pymysql

    conn = BaseHook.get_connection(MYSQL_CONN_ID)
    return pymysql.connect(host=conn.host, port=int(conn.port or 3306), user=conn.login,
                           password=conn.password, database=conn.schema, charset='utf8mb4')


def staging_ddl(table):
    """DDL of the staging table: same file as the live table, only the table name differs."""
    return re.sub(r'CREATE TABLE IF NOT EXISTS (\w+)', r'CREATE TABLE \1' + STAGING_SUFFIX,
                  read_ddl('mysql', table), count=1)


def prepare_mysql_tables():
    """
    Creates the live tables if they do not exist yet (so the frontend works
    before the first load) and recreates empty staging tables from the DDLs.
    """
    connection = mysql_connect()
    try:
        with connection.cursor() as cursor:
            for table in MYSQL_TABLES:
                cursor.execute(read_ddl('mysql', table))
                cursor.execute('DROP TABLE IF EXISTS ' + table + STAGING_SUFFIX)
                cursor.execute(staging_ddl(table))
        connection.commit()
    finally:
        connection.close()


def publish_mysql_tables(full_run):
    """
    Checks the staging tables and swaps them with the live tables.

    RENAME TABLE swaps all tables in one atomic step, so the frontend sees
    either the old or the new data, never a half-loaded table.
    Checks: staging must not be empty, and a full run must not have more
    than 10 % fewer cards than the current data (e.g. API answered incompletely).
    Test runs (mtg_max_pages > 0) skip the size check.
    """
    connection = mysql_connect()
    try:
        with connection.cursor() as cursor:
            cursor.execute('SELECT COUNT(*) FROM cards' + STAGING_SUFFIX)
            new_count = cursor.fetchone()[0]
            cursor.execute('SELECT COUNT(*) FROM cards')
            old_count = cursor.fetchone()[0]
            print('cards%s: %d rows, cards (live): %d rows' % (STAGING_SUFFIX, new_count, old_count))

            if new_count == 0:
                raise ValueError('Staging table is empty, publishing aborted')
            if full_run and old_count and new_count < old_count * MIN_RATIO_FULL_RUN:
                raise ValueError('New data (%d cards) is more than %d %% smaller than current data (%d cards)'
                                 % (new_count, round((1 - MIN_RATIO_FULL_RUN) * 100), old_count))

            renames = []
            for table in MYSQL_TABLES:
                cursor.execute('DROP TABLE IF EXISTS %s_old' % table)  # leftover of an aborted run
                renames +=['%s TO %s_old' % (table, table), '%s%s TO %s' % (table, STAGING_SUFFIX, table)]
            cursor.execute('RENAME TABLE ' + ', '.join(renames))
            for table in MYSQL_TABLES:
                cursor.execute('DROP TABLE %s_old' % table)
        connection.commit()
        print('Published %d cards' % new_count)
    finally:
        connection.close()


args = {
    'owner': 'airflow'
}

dag = DAG('MTG', default_args=args, description='MTG Trading Card Database By API',
          schedule_interval='0 3 * * *',
          start_date=datetime(2026, 11, 14), catchup=False, max_active_runs=1)

# 1. Get MTG data from API

create_local_import_dir = CreateDirectoryOperator(
    task_id='create_import_dir',
    path='/home/airflow',
    directory='mtg',
    dag=dag,
)

clear_local_import_dir = ClearDirectoryOperator(
    task_id='clear_import_dir',
    directory=LOCAL_IMPORT_DIR,
    pattern='*',
    dag=dag,
)

download_mtg_cards = BashOperator(
    task_id='download_mtg_cards',
    bash_command='python3 ' + AIRFLOW_HOME + '/python/download_mtg_cards.py'
                 ' --target_dir ' + LOCAL_IMPORT_DIR +
                 ' --max_pages ' + MAX_PAGES + ' ',
    dag=dag,
)

# 2. Save raw data (JSON) to HDFS

clear_hdfs_raw_partition_dir = BashOperator(
    task_id='clear_hdfs_raw_partition_dir',
    bash_command='hadoop fs -rm -r -f ' + HDFS_RAW_PARTITION_DIR + ' ',
    dag=dag,
)

create_hdfs_raw_partition_dir = HdfsMkdirFileOperator(
    task_id='mkdir_hdfs_raw_partition_dir',
    directory=HDFS_RAW_PARTITION_DIR,
    hdfs_conn_id='hdfs',
    dag=dag,
)

hdfs_put_raw_cards = BashOperator(
    task_id='upload_raw_cards_to_hdfs',
    bash_command='hadoop fs -put -f ' + LOCAL_IMPORT_DIR + '/*.json ' + HDFS_RAW_PARTITION_DIR + '/ ',
    dag=dag,
)

# 3. Raw to final: flatten, reduce, clean, deduplicate

pyspark_raw_to_final = SparkSubmitOperator(
    task_id='pyspark_raw_to_final',
    conn_id='spark',
    application=AIRFLOW_HOME + '/python/pyspark_mtg_raw_to_final.py',
    total_executor_cores='2',
    executor_cores='2',
    executor_memory='2g',
    num_executors='2',
    name='spark_mtg_raw_to_final',
    verbose=True,
    application_args=['--year', YEAR, '--month', MONTH, '--day', DAY,
                      '--hdfs_source_dir', HDFS_RAW_DIR,
                      '--hdfs_target_dir', HDFS_FINAL_DIR],
    dag=dag,
)

create_hive_table_mtg_cards = HiveOperator(
    task_id='create_hive_table_mtg_cards',
    hql=read_ddl('hive', 'mtg_cards'),
    hive_cli_conn_id='beeline',
    dag=dag,
)

create_hive_table_mtg_foreign_names = HiveOperator(
    task_id='create_hive_table_mtg_foreign_names',
    hql=read_ddl('hive', 'mtg_foreign_names'),
    hive_cli_conn_id='beeline',
    dag=dag,
)

# 4. Enhance data and export to end-user database (MySQL): load staging, check, swap

prepare_mysql_tables_task = PythonOperator(
    task_id='prepare_mysql_tables',
    python_callable=prepare_mysql_tables,
    dag=dag,
)

pyspark_final_to_mysql = SparkSubmitOperator(
    task_id='pyspark_final_to_mysql',
    conn_id='spark',
    application=AIRFLOW_HOME + '/python/pyspark_mtg_final_to_mysql.py',
    jars=MYSQL_JDBC_JAR,
    total_executor_cores='2',
    executor_cores='2',
    executor_memory='2g',
    num_executors='2',
    name='spark_mtg_final_to_mysql',
    verbose=False,
    application_args=['--hdfs_source_dir', HDFS_FINAL_DIR,
                      '--load_date', '{{ ds }}',
                      '--mysql_env_file', MYSQL_ENV_FILE,
                      '--table_suffix', STAGING_SUFFIX],
    dag=dag,
)

publish_mysql_tables_task = PythonOperator(
    task_id='publish_mysql_tables',
    python_callable=publish_mysql_tables,
    op_kwargs={'full_run': MAX_PAGES.strip() == '0'},
    dag=dag,
)

# 5. Housekeeping: keep only the newest raw partitions (YYYY/MM/DD sorts chronologically)

cleanup_hdfs_raw = BashOperator(
    task_id='cleanup_hdfs_raw',
    bash_command="hadoop fs -ls -d '" + HDFS_RAW_DIR + "/*/*/*' | awk '{print $NF}' | grep '^/' | sort"
                 " | head -n -" + str(KEEP_RAW_DAYS) + " | xargs -r hadoop fs -rm -r -skipTrash ",
    dag=dag,
)

create_local_import_dir >> clear_local_import_dir >> download_mtg_cards
download_mtg_cards >> clear_hdfs_raw_partition_dir >> create_hdfs_raw_partition_dir >> hdfs_put_raw_cards
hdfs_put_raw_cards >> pyspark_raw_to_final
pyspark_raw_to_final >> create_hive_table_mtg_cards >> prepare_mysql_tables_task
pyspark_raw_to_final >> create_hive_table_mtg_foreign_names >> prepare_mysql_tables_task
prepare_mysql_tables_task >> pyspark_final_to_mysql >> publish_mysql_tables_task >> cleanup_hdfs_raw
