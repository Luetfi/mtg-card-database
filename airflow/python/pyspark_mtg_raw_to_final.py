# -*- coding: utf-8 -*-

"""
Raw To Final Transfer for MTG card data.

Reads the raw API responses (one JSON document per page, {"cards": [...]})
from HDFS, flattens them to one row per card, keeps only the attributes
needed for searching, cleans them, removes duplicates and writes the result
as Parquet to the final directory on HDFS:

    <hdfs_target_dir>/cards
    <hdfs_target_dir>/foreign_names
"""

import argparse

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, DoubleType, StringType, StructField, StructType


def get_args():
    parser = argparse.ArgumentParser(description='Transfers MTG card data from raw to final layer.')
    parser.add_argument('--year', help='Partition Year To Process, e.g. 2026', required=True, type=str)
    parser.add_argument('--month', help='Partition Month To Process, e.g. 11', required=True, type=str)
    parser.add_argument('--day', help='Partition Day To Process, e.g. 15', required=True, type=str)
    parser.add_argument('--hdfs_source_dir', help='HDFS raw directory, e.g. /user/hadoop/mtg/raw', required=True, type=str)
    parser.add_argument('--hdfs_target_dir', help='HDFS final directory, e.g. /user/hadoop/mtg/final', required=True, type=str)
    return parser.parse_args()


# Explicit schema: only the attributes we need are read, everything else
# (rulings, legalities, printings, foreign texts...) is dropped while parsing.
# IDs are read as strings, because the API delivers them as number or string.
FOREIGN_NAME_SCHEMA = StructType([
    StructField('name', StringType()),
    StructField('language', StringType()),
    StructField('multiverseid', StringType()),
    StructField('imageUrl', StringType()),
])

CARD_SCHEMA = StructType([
    StructField('id', StringType()),
    StructField('multiverseid', StringType()),
    StructField('name', StringType()),
    StructField('manaCost', StringType()),
    StructField('cmc', DoubleType()),
    StructField('colors', ArrayType(StringType())),
    StructField('colorIdentity', ArrayType(StringType())),
    StructField('type', StringType()),
    StructField('supertypes', ArrayType(StringType())),
    StructField('types', ArrayType(StringType())),
    StructField('subtypes', ArrayType(StringType())),
    StructField('rarity', StringType()),
    StructField('set', StringType()),
    StructField('setName', StringType()),
    StructField('text', StringType()),
    StructField('flavor', StringType()),
    StructField('artist', StringType()),
    StructField('number', StringType()),
    StructField('power', StringType()),
    StructField('toughness', StringType()),
    StructField('loyalty', StringType()),
    StructField('layout', StringType()),
    StructField('imageUrl', StringType()),
    StructField('foreignNames', ArrayType(FOREIGN_NAME_SCHEMA)),
])

RAW_SCHEMA = StructType([StructField('cards', ArrayType(CARD_SCHEMA))])


def clean(column):
    """Trims a string column and turns empty strings into NULL."""
    trimmed = F.trim(column)
    return F.when(trimmed == '', None).otherwise(trimmed)


if __name__ == '__main__':
    args = get_args()

    sc = pyspark.SparkContext()
    spark = SparkSession(sc)

    raw_path = '%s/%s/%s/%s/*.json' % (args.hdfs_source_dir, args.year, args.month, args.day)

    # Each file is one multi-line JSON document
    raw = spark.read.schema(RAW_SCHEMA).option('multiLine', 'true').json(raw_path)

    # One row per card
    exploded = raw.select(F.explode('cards').alias('c')).select('c.*')

    # Business rules:
    # - cards without id or name are unusable and dropped
    # - a card id may only occur once (duplicates removed)
    cards_all = exploded\
        .filter(F.col('id').isNotNull() & F.col('name').isNotNull())\
        .dropDuplicates(['id'])\
        .cache()

    cards = cards_all.select(
        F.col('id'),
        F.col('multiverseid').cast('int').alias('multiverse_id'),
        clean(F.col('name')).alias('name'),
        clean(F.col('manaCost')).alias('mana_cost'),
        F.col('cmc'),
        F.concat_ws(',', 'colors').alias('colors'),
        F.concat_ws(',', 'colorIdentity').alias('color_identity'),
        clean(F.col('type')).alias('type'),
        F.concat_ws(',', 'supertypes').alias('supertypes'),
        F.concat_ws(',', 'types').alias('types'),
        F.concat_ws(',', 'subtypes').alias('subtypes'),
        clean(F.col('rarity')).alias('rarity'),
        clean(F.col('set')).alias('set_code'),
        clean(F.col('setName')).alias('set_name'),
        clean(F.col('text')).alias('text'),
        clean(F.col('flavor')).alias('flavor'),
        clean(F.col('artist')).alias('artist'),
        clean(F.col('number')).alias('number'),
        clean(F.col('power')).alias('power'),
        clean(F.col('toughness')).alias('toughness'),
        clean(F.col('loyalty')).alias('loyalty'),
        clean(F.col('layout')).alias('layout'),
        clean(F.col('imageUrl')).alias('image_url'),
    )

    # Translated names: one row per card and language
    foreign_names = cards_all\
        .select(F.col('id').alias('card_id'), F.explode('foreignNames').alias('f'))\
        .select(
            F.col('card_id'),
            clean(F.col('f.language')).alias('language'),
            clean(F.col('f.name')).alias('name'),
            F.col('f.multiverseid').cast('int').alias('multiverse_id'),
            clean(F.col('f.imageUrl')).alias('image_url'),
        )\
        .filter(F.col('name').isNotNull() & F.col('language').isNotNull())\
        .dropDuplicates(['card_id', 'language', 'name'])

    cards.coalesce(4).write.format('parquet').mode('overwrite').save(args.hdfs_target_dir + '/cards')
    foreign_names.coalesce(4).write.format('parquet').mode('overwrite').save(args.hdfs_target_dir + '/foreign_names')

    print('Raw to final finished: %d cards, %d foreign names'
          % (spark.read.parquet(args.hdfs_target_dir + '/cards').count(),
             spark.read.parquet(args.hdfs_target_dir + '/foreign_names').count()))
