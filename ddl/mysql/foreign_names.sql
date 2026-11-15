-- MySQL: Endnutzer-Datenbank, uebersetzte Kartennamen (eine Zeile pro Karte und Sprache)
CREATE TABLE IF NOT EXISTS foreign_names (
    card_id        VARCHAR(64)   NOT NULL,
    language       VARCHAR(50)   NOT NULL,
    name           VARCHAR(255)  NOT NULL,
    multiverse_id  INT           NULL,
    image_url      VARCHAR(500)  NULL,
    KEY idx_foreign_names_card_id (card_id),
    KEY idx_foreign_names_name (name)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
