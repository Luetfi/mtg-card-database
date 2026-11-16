# -*- coding: utf-8 -*-

"""
Enhance MTG card data and export it to the end-user database (MySQL).

Reads the final layer from HDFS, adds attributes that help the frontend and
writes both tables into staging tables (cards_staging, foreign_names_staging).
The DAG task publish_mysql_tables checks the staging tables and swaps them
with the live tables in one step, so the frontend never sees a half-loaded
database.

Enhancements:
- has_image:       1 if the card has a picture (shown first in the frontend)
- printings_count: in how many printings (sets) a card name exists
- load_date:       when the data was loaded
- color_names:     colors in German (White -> Weiss ...)
- name_de:         German card name (from foreignNames)
- search_name:     normalized English + German name (lower case, no accents)
- search_all:      normalized name, German name, card text and artist

The MySQL credentials are read from a file (KEY=VALUE lines), so the password
never appears on the command line, in the Airflow logs or in the Airflow UI.
"""

import argparse

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import Window
from pyspark.sql import functions as F

# Normalization for searching. The frontend (frontend/app.py, normalize())
# uses exactly the same rules for the user input.
REPLACEMENTS = [(u'ß', u'ss'), (u'æ', u'ae'), (u'œ', u'oe')]   # ss, ae, oe
ACCENTS_FROM = u'àáâãäåçèéêëìíîï' \
               u'ñòóôõöøùúûüýÿ'
ACCENTS_TO = u'aaaaaaceeeeiiiinoooooouuuuyy'

# The API delivers colors as letters (W, U, B, R, G) or, in older data, as full names
COLOR_NAMES = [(r'\b(W|White)\b', u'Weiß'), (r'\b(U|Blue)\b', u'Blau'), (r'\b(B|Black)\b', u'Schwarz'),
               (r'\b(R|Red)\b', u'Rot'), (r'\b(G|Green)\b', u'Grün')]


def get_args():
    parser = argparse.ArgumentParser(description='Exports MTG card data from final layer to MySQL.')
    parser.add_argument('--hdfs_source_dir', help='HDFS final directory, e.g. /user/hadoop/mtg/final', required=True, type=str)
    parser.add_argument('--load_date', help='Load date, e.g. 2026-11-15', required=True, type=str)
    parser.add_argument('--mysql_env_file', help='File with MYSQL_HOST, MYSQL_PORT, MYSQL_DATABASE, MYSQL_USER, MYSQL_PASSWORD',
                        required=True, type=str)
    parser.add_argument('--table_suffix', help='Suffix of the target tables, e.g. _staging', required=False, type=str, default='_staging')
    return parser.parse_args()


def read_env_file(path):
    settings = {}
    with open(path) as env_file:
        for line in env_file:
            line = line.strip()
            if line and not line.startswith('#') and '=' in line:
                key, value = line.split('=', 1)
                settings[key.strip()] = value.strip()
    return settings


def normalize(column):
    """Lower case, umlauts and accents removed, e.g. 'Weiss' and 'weiß' -> 'weiss'."""
    result = F.lower(column)
    for source, target in REPLACEMENTS:
        result = F.regexp_replace(result, source, target)
    return F.translate(result, ACCENTS_FROM, ACCENTS_TO)


def german_colors(column):
    result = column
    for pattern, german in COLOR_NAMES:
        result = F.regexp_replace(result, pattern, german)
    return F.when(result == '', None).otherwise(result)


def write_to_mysql(dataframe, table, mysql):
    url = 'jdbc:mysql://%s:%s/%s?useUnicode=true&characterEncoding=UTF-8&useSSL=false' \
          '&allowPublicKeyRetrieval=true&rewriteBatchedStatements=true' \
          % (mysql['MYSQL_HOST'], mysql.get('MYSQL_PORT', '3306'), mysql['MYSQL_DATABASE'])
    # The (empty) staging table was created by the DAG from the DDL file,
    # so "append" keeps column types and indexes from ddl/mysql/<table>.sql.
    dataframe.write.format('jdbc')\
        .option('url', url)\
        .option('dbtable', table)\
        .option('user', mysql['MYSQL_USER'])\
        .option('password', mysql['MYSQL_PASSWORD'])\
        .option('driver', 'com.mysql.cj.jdbc.Driver')\
        .option('batchsize', '5000')\
        .mode('append')\
        .save()


if __name__ == '__main__':
    args = get_args()
    mysql = read_env_file(args.mysql_env_file)

    sc = pyspark.SparkContext()
    spark = SparkSession(sc)

    cards = spark.read.parquet(args.hdfs_source_dir + '/cards')
    foreign_names = spark.read.parquet(args.hdfs_source_dir + '/foreign_names')

    # German name per card (a card has at most one German name; min() makes it deterministic)
    german_names = foreign_names\
        .filter(F.col('language') == 'German')\
        .groupBy('card_id')\
        .agg(F.min('name').alias('name_de'))

    cards_enhanced = cards\
        .join(german_names, cards['id'] == german_names['card_id'], 'left')\
        .drop('card_id')\
        .withColumn('has_image', F.when(F.col('image_url').isNotNull(), 1).otherwise(0))\
        .withColumn('printings_count', F.count('id').over(Window.partitionBy('name')).cast('int'))\
        .withColumn('load_date', F.lit(args.load_date).cast('date'))\
        .withColumn('color_names', german_colors(F.col('colors')))\
        .withColumn('search_name', normalize(F.concat_ws(' | ', 'name', 'name_de')))\
        .withColumn('search_all', normalize(F.concat_ws(' | ', 'name', 'name_de', 'text', 'artist')))\
        .cache()

    write_to_mysql(cards_enhanced.repartition(4), 'cards' + args.table_suffix, mysql)
    write_to_mysql(foreign_names.repartition(4), 'foreign_names' + args.table_suffix, mysql)

    print('Export finished: %d cards (%d with German name), %d foreign names'
          % (cards_enhanced.count(), cards_enhanced.filter(F.col('name_de').isNotNull()).count(),
             foreign_names.count()))
