# -*- coding: utf-8 -*-

"""
Enhance MTG card data and export it to the end-user database (MySQL).

Reads the final layer from HDFS, adds attributes that help the frontend
(has_image, printings_count, load_date) and writes both tables to MySQL.
The tables are truncated and refilled, so the table definitions (DDL,
indexes) stay untouched and every run leaves a complete, current data set.
"""

import argparse

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import Window
from pyspark.sql import functions as F


def get_args():
    parser = argparse.ArgumentParser(description='Exports MTG card data from final layer to MySQL.')
    parser.add_argument('--hdfs_source_dir', help='HDFS final directory, e.g. /user/hadoop/mtg/final', required=True, type=str)
    parser.add_argument('--load_date', help='Load date, e.g. 2026-11-15', required=True, type=str)
    parser.add_argument('--mysql_host', required=True, type=str)
    parser.add_argument('--mysql_port', required=False, type=str, default='3306')
    parser.add_argument('--mysql_database', required=True, type=str)
    parser.add_argument('--mysql_user', required=True, type=str)
    parser.add_argument('--mysql_password', required=True, type=str)
    return parser.parse_args()


def write_to_mysql(dataframe, table, args):
    url = 'jdbc:mysql://%s:%s/%s?useUnicode=true&characterEncoding=UTF-8&useSSL=false' \
          '&allowPublicKeyRetrieval=true&rewriteBatchedStatements=true' \
          % (args.mysql_host, args.mysql_port, args.mysql_database)
    dataframe.write.format('jdbc')\
        .option('url', url)\
        .option('dbtable', table)\
        .option('user', args.mysql_user)\
        .option('password', args.mysql_password)\
        .option('driver', 'com.mysql.cj.jdbc.Driver')\
        .option('truncate', 'true')\
        .option('batchsize', '5000')\
        .mode('overwrite')\
        .save()


if __name__ == '__main__':
    args = get_args()

    sc = pyspark.SparkContext()
    spark = SparkSession(sc)

    cards = spark.read.parquet(args.hdfs_source_dir + '/cards')
    foreign_names = spark.read.parquet(args.hdfs_source_dir + '/foreign_names')

    # Enhance data for the frontend:
    # - has_image: cards with a picture are shown first
    # - printings_count: in how many printings (sets) a card name exists
    # - load_date: when the data was loaded
    cards_enhanced = cards\
        .withColumn('has_image', F.when(F.col('image_url').isNotNull(), 1).otherwise(0))\
        .withColumn('printings_count', F.count('id').over(Window.partitionBy('name')).cast('int'))\
        .withColumn('load_date', F.lit(args.load_date).cast('date'))

    write_to_mysql(cards_enhanced.repartition(4), 'cards', args)
    write_to_mysql(foreign_names.repartition(4), 'foreign_names', args)

    print('Export finished: %d cards, %d foreign names' % (cards_enhanced.count(), foreign_names.count()))
