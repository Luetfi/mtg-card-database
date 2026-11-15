# MTG Trading Card Database By API

Praktische Prüfung Big Data, Wintersemester 2026/2027, DHBW Stuttgart.

Ziel: Aus den Daten von [api.magicthegathering.io](https://docs.magicthegathering.io/) entsteht eine durchsuchbare Datenbank aller Magic-The-Gathering-Karten. Der gesamte Datenfluss läuft automatisiert als Airflow-DAG.

## 1. Überblick

```
api.magicthegathering.io
        |  download_mtg_cards.py (HTTP GET, 100 Karten pro Seite)
        v
lokal: /home/airflow/mtg/cards_page_*.json
        |  hadoop fs -put
        v
HDFS raw:   /user/hadoop/mtg/raw/YYYY/MM/DD/*.json      (unveränderte API-Antworten)
        |  PySpark: pyspark_mtg_raw_to_final.py
        v
HDFS final: /user/hadoop/mtg/final/cards                (Parquet, Hive-Tabelle mtg_cards)
            /user/hadoop/mtg/final/foreign_names        (Parquet, Hive-Tabelle mtg_foreign_names)
        |  PySpark: pyspark_mtg_final_to_mysql.py (JDBC)
        v
MySQL (Datenbank mtg): cards, foreign_names
        |  SQL
        v
Frontend (Flask): Suche nach Kartenname, Text oder Artist
```

| Komponente | Container | Aufgabe |
|---|---|---|
| Hadoop, YARN, Hive, Spark | `hadoop` (`marcelmittelstaedt/spark_base`) | HDFS-Speicher, Ausführung der Spark-Jobs, HiveServer2 |
| Airflow 1.10 | `airflow` (`marcelmittelstaedt/airflow`) | ETL-Workflow, Scheduling |
| MySQL 8.0 | `mysql` | Endnutzer-Datenbank |
| Frontend | `mtg-frontend` | Suchoberfläche, Port 5000 |

Alle Container laufen im Docker-Netz `bigdatanet` auf einer Google-Cloud-VM.

## 2. Datenquelle

- Endpunkt: `GET https://api.magicthegathering.io/v1/cards?page=N&pageSize=100`
- Antwort: ein JSON-Dokument `{"cards": [ {...}, ... ]}` mit bis zu 100 Karten.
- Die Gesamtzahl der Karten steht im Antwort-Header `Total-Count` (aktuell rund 93.600 Karten, also rund 940 Seiten).
- Die API ist rate-limitiert (Header `Ratelimit-Limit: 1000`). Ein Komplettabzug braucht rund 940 Requests.
- Jede Karte ist ein *Druck* (Printing): Dieselbe Karte kommt in mehreren Sets vor und hat pro Set eine eigene `id`.
- Übersetzte Namen stehen im verschachtelten Feld `foreignNames`.

Vorgehen beim Abruf: Die erste Seite liefert die Gesamtzahl, daraus ergibt sich die Seitenzahl. Die restlichen Seiten werden mit vier parallelen Downloads geholt. Jede Antwort wird unverändert als eigene Datei gespeichert. Bei Fehlern oder erreichtem Rate-Limit wird der Request mit wachsender Pause wiederholt.

## 3. Airflow-DAG `MTG`

Datei: [airflow/dags/mtg.py](airflow/dags/mtg.py). Der DAG läuft automatisch täglich um 03:00 Uhr UTC (`schedule_interval='0 3 * * *'`) und kann in der Airflow-Oberfläche manuell gestartet werden.

| # | Task | Operator | Zweck |
|---|---|---|---|
| 1 | `create_import_dir` | `CreateDirectoryOperator` | Legt das lokale Importverzeichnis `/home/airflow/mtg` an. |
| 2 | `clear_import_dir` | `ClearDirectoryOperator` | Leert das Importverzeichnis, damit keine Dateien eines früheren Laufs übrig bleiben. |
| 3 | `download_mtg_cards` | `BashOperator` | Ruft `download_mtg_cards.py` auf und lädt alle Karten seitenweise von der API. |
| 4 | `clear_hdfs_raw_partition_dir` | `BashOperator` | Löscht die Raw-Partition des Lauftags in HDFS, damit ein wiederholter Lauf keine Dubletten erzeugt. |
| 5 | `mkdir_hdfs_raw_partition_dir` | `HdfsMkdirFileOperator` | Legt die Raw-Partition `/user/hadoop/mtg/raw/YYYY/MM/DD` an. |
| 6 | `upload_raw_cards_to_hdfs` | `BashOperator` | Kopiert die JSON-Dateien unverändert nach HDFS. |
| 7 | `pyspark_raw_to_final` | `SparkSubmitOperator` | Überführt die Rohdaten in den Final Layer (siehe Business Rules). |
| 8 | `create_hive_table_mtg_cards` | `HiveOperator` | Legt die externe Hive-Tabelle `mtg_cards` über dem Final Layer an. |
| 9 | `create_hive_table_mtg_foreign_names` | `HiveOperator` | Legt die externe Hive-Tabelle `mtg_foreign_names` an. |
| 10 | `create_mysql_tables` | `PythonOperator` | Legt die MySQL-Tabellen `cards` und `foreign_names` an, falls sie fehlen. |
| 11 | `pyspark_final_to_mysql` | `SparkSubmitOperator` | Reichert die Daten an und exportiert sie nach MySQL. |

Reihenfolge: 1 -> 2 -> 3 -> 4 -> 5 -> 6 -> 7 -> (8 und 9 parallel) -> 10 -> 11.

Einstellungen in Airflow:
- **Variable `mtg_max_pages`:** `0` lädt alle Seiten (Standard). Eine andere Zahl begrenzt den Abruf, z. B. `5` für einen schnellen Testlauf.
- **Connection `mtg_mysql`:** Zugangsdaten der MySQL-Datenbank. Der DAG liest sie zur Laufzeit; im Code stehen keine Passwörter.
- **Connections `hdfs`, `spark`, `beeline`:** sind im Airflow-Image der Vorlesung bereits vorhanden.

## 4. Skripte

| Datei | Zweck |
|---|---|
| [airflow/python/download_mtg_cards.py](airflow/python/download_mtg_cards.py) | Lädt die Karten seitenweise von der API und speichert jede Antwort als `cards_page_NNNNN.json`. |
| [airflow/python/pyspark_mtg_raw_to_final.py](airflow/python/pyspark_mtg_raw_to_final.py) | Spark-Job Raw -> Final. |
| [airflow/python/pyspark_mtg_final_to_mysql.py](airflow/python/pyspark_mtg_final_to_mysql.py) | Spark-Job Final -> MySQL. |

### Business Rules Raw -> Final

- **Flatten:** Aus einem JSON-Dokument pro Seite wird eine Zeile pro Karte (`explode`).
- **Reduce:** Ein festes Schema liest nur die Attribute, die für die Suche gebraucht werden. Regeln (`rulings`), Legalitäten, Drucklisten und übersetzte Texte entfallen.
- **Clean:** Texte werden getrimmt, leere Strings werden zu `NULL`. Listen (Farben, Typen) werden zu kommagetrennten Strings. `multiverseid` wird zu `INT` vereinheitlicht, weil die API das Feld mal als Zahl und mal als String liefert.
- **Filter:** Karten ohne `id` oder ohne `name` werden verworfen.
- **Deduplicate:** Jede Karten-`id` kommt genau einmal vor.
- **Übersetzungen:** `foreignNames` wird in eine eigene Tabelle ausgelagert (eine Zeile pro Karte und Sprache, Dubletten entfernt).
- **Format:** Parquet, weil es spaltenorientiert und komprimiert ist und das Schema mitführt.

### Business Rules Final -> MySQL

- `has_image`: 1, wenn die Karte eine Bild-URL hat. Das Frontend zeigt Karten mit Bild zuerst.
- `printings_count`: Anzahl der Drucke mit demselben Kartennamen.
- `load_date`: Datum des Laufs.
- Die Tabellen werden pro Lauf geleert und neu gefüllt (`TRUNCATE` und Insert). So bleibt die Tabellendefinition samt Indizes erhalten, und der Stand ist nach jedem Lauf vollständig und aktuell.

## 5. Datenmodell

**Raw Layer (HDFS):** `/user/hadoop/mtg/raw/YYYY/MM/DD/cards_page_NNNNN.json`. Unveränderte API-Antworten, partitioniert nach Ladedatum.

**Final Layer (HDFS und Hive):**
- [ddl/hive/mtg_cards.sql](ddl/hive/mtg_cards.sql): eine Zeile pro Kartendruck.
- [ddl/hive/mtg_foreign_names.sql](ddl/hive/mtg_foreign_names.sql): eine Zeile pro Karte und Sprache.

**Endnutzer-Datenbank (MySQL, Datenbank `mtg`):**
- [ddl/mysql/cards.sql](ddl/mysql/cards.sql): Karten mit den Zusatzspalten `has_image`, `printings_count`, `load_date`. Primärschlüssel `id`, Indizes auf `name`, `artist`, `multiverse_id`.
- [ddl/mysql/foreign_names.sql](ddl/mysql/foreign_names.sql): übersetzte Namen, Indizes auf `card_id` und `name`.

## 6. Frontend

Code: [frontend/app.py](frontend/app.py) und [frontend/templates/index.html](frontend/templates/index.html), ausgeliefert als Docker-Container ([frontend/Dockerfile](frontend/Dockerfile)).

- Liest aus der MySQL-Datenbank.
- Verarbeitet die Nutzereingabe: Suchbegriff und Suchfeld (Kartenname, Kartentext, Artist oder alle drei).
- Die Namenssuche findet auch übersetzte Namen: "Manta" findet "Manta Riders" und über den deutschen Namen "Wolkenmanta" auch die Karte "Cloud Manta".
- Zeigt Bild, Name, Typ, Set, Text, Artist und ID (Multiverse-ID), 50 Treffer pro Seite.
- Die Suche ist eine Teilstring-Suche (`LIKE %begriff%`) mit gebundenen Parametern.

Aufruf: `http://<VM-IP>:5000/`

## 7. Installation und Betrieb

Voraussetzung: Die Container `hadoop` und `airflow` laufen wie in Übung 5 der Vorlesung im Netz `bigdatanet`, Hadoop und HiveServer2 sind gestartet.

```bash
# einmalig auf der VM, im Verzeichnis mtg_exam
./setup/setup_vm.sh
```

Das Skript startet MySQL und das Frontend, installiert `pymysql` und den MySQL-JDBC-Treiber im Airflow-Container, legt die Airflow-Connection `mtg_mysql` an und kopiert DAG, Skripte und DDLs nach Airflow. Die Datenbank-Passwörter werden dabei einmalig zufällig erzeugt und in `~/mtg.env` auf der VM abgelegt.

| Skript | Zweck |
|---|---|
| [setup/setup_vm.sh](setup/setup_vm.sh) | Einmalige Einrichtung (siehe oben). |
| [setup/deploy.sh](setup/deploy.sh) | Kopiert DAG, Skripte und DDLs erneut in den Airflow-Container. |
| [setup/start_services.sh](setup/start_services.sh) | Startet nach einem VM-Neustart Container, Hadoop und HiveServer2. |
| [setup/dag_status.sh](setup/dag_status.sh) | Zeigt den Status der DAG-Läufe und Tasks (im Airflow-Container ausführen). |

Workflow starten: in der Airflow-Oberfläche (`http://<VM-IP>:8080/admin/`) den DAG `MTG` auslösen oder im Airflow-Container `airflow trigger_dag MTG` ausführen.
