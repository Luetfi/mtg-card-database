-- Hive: Final Layer, eine Zeile pro Kartendruck (Printing)
CREATE EXTERNAL TABLE IF NOT EXISTS mtg_cards (
    id STRING,
    multiverse_id INT,
    name STRING,
    mana_cost STRING,
    cmc DOUBLE,
    colors STRING,
    color_identity STRING,
    type STRING,
    supertypes STRING,
    types STRING,
    subtypes STRING,
    rarity STRING,
    set_code STRING,
    set_name STRING,
    text STRING,
    flavor STRING,
    artist STRING,
    number STRING,
    power STRING,
    toughness STRING,
    loyalty STRING,
    layout STRING,
    image_url STRING
) COMMENT 'MTG Cards (final layer)'
STORED AS PARQUET
LOCATION '/user/hadoop/mtg/final/cards'
