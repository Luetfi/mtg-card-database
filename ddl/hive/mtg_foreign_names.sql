-- Hive: Final Layer, uebersetzte Kartennamen (eine Zeile pro Karte und Sprache)
CREATE EXTERNAL TABLE IF NOT EXISTS mtg_foreign_names (
    card_id STRING,
    language STRING,
    name STRING,
    multiverse_id INT,
    image_url STRING
) COMMENT 'MTG Foreign Card Names (final layer)'
STORED AS PARQUET
LOCATION '/user/hadoop/mtg/final/foreign_names'
