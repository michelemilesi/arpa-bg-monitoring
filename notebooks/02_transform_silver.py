# Databricks notebook source
# MAGIC %md
# MAGIC # 02 - Spark Declarative Pipeline (Silver Cleansing & Auto CDC SCD2)
# MAGIC 
# MAGIC This notebook implements the **Silver Layer** of the Spark Declarative Pipeline using `pyspark.pipelines`:
# MAGIC 1. Cleans, validates, and filters data specifically for the **Province of Bergamo (`provincia = 'BG'`)**.
# MAGIC 2. Removes sensor anomaly readings and sentinel error values (e.g., negative concentrations `-999`).
# MAGIC 3. Enriches measurement records with temporal features (`anno`, `mese`, `giorno`, `ora`, `giorno_settimana`, `is_weekend`).
# MAGIC 4. Applies **Auto CDC SCD Type 2 (SCD2)** on both `stazioni_aria` and `rilevazioni_aria` within the Silver schema (`silver_schema`) via `dp.create_auto_cdc_flow()`.
# MAGIC 5. Defines explicit schemas with comprehensive English descriptions for tables and columns.

# COMMAND ----------

from pyspark import pipelines as dp
from pyspark.sql.functions import (
    col,
    dayofmonth,
    dayofweek,
    date_format,
    hour,
    month,
    to_timestamp,
    trim,
    upper,
    when,
    year,
)

# Configuration for multi-schema resolution
bronze_schema = spark.conf.get("bronze_schema", "dev_bronze")
silver_schema = spark.conf.get("silver_schema", "dev_silver")

target_stazioni_silver = f"{silver_schema}.stazioni_aria"
target_rilevazioni_silver = f"{silver_schema}.rilevazioni_aria"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Silver Stations & Sensors: Cleansing & Auto CDC (SCD2)

# COMMAND ----------

SILVER_STAZIONI_SCHEMA = """
    idsensore STRING COMMENT 'Unique identifier of the ARPA Lombardia air quality sensor (business primary key)',
    nomestazione STRING COMMENT 'Normalized monitoring station name',
    idstazione STRING COMMENT 'Unique identifier of the monitoring station',
    comune STRING COMMENT 'Cleaned municipality name in the Province of Bergamo',
    provincia STRING COMMENT 'Normalized province code (always BG)',
    lat DOUBLE COMMENT 'Validated geographic latitude coordinate (WGS84)',
    lng DOUBLE COMMENT 'Validated geographic longitude coordinate (WGS84)',
    quota DOUBLE COMMENT 'Station elevation above sea level in meters',
    nometiposensore STRING COMMENT 'Standardized monitored pollutant parameter name',
    unitamisura STRING COMMENT 'Standardized unit of measurement (e.g., µg/m³)',
    datastart TIMESTAMP COMMENT 'Sensor activation start timestamp',
    datastop TIMESTAMP COMMENT 'Sensor deactivation timestamp (NULL if currently active)',
    is_attivo BOOLEAN COMMENT 'Flag indicating if the sensor is currently active (datastop is null)',
    _ingestion_ts TIMESTAMP COMMENT 'Ingestion timestamp propagated from bronze',
    __START_AT TIMESTAMP COMMENT 'Record validity start timestamp managed automatically by Auto CDC (SCD Type 2)',
    __END_AT TIMESTAMP COMMENT 'Record validity end timestamp managed automatically by Auto CDC (SCD Type 2, NULL for current active record)'
"""

@dp.view(
    name="silver_stazioni_aria_clean",
    comment="Streaming view filtering and cleansing station metadata for Bergamo province"
)
@dp.expect_or_drop("valid_idsensore_silver", "idsensore IS NOT NULL")
@dp.expect_or_drop("valid_bergamo_province", "provincia = 'BG'")
@dp.expect("valid_coordinates_bergamo", "lat BETWEEN 45.0 AND 46.5 AND lng BETWEEN 9.3 AND 10.3")
def silver_stazioni_aria_clean():
    """
    Cleans raw stations stream:
    - Filters strictly for Province of Bergamo ('BG')
    - Cleans whitespace and normalizes text fields
    - Converts start and end dates to TIMESTAMP
    - Adds boolean is_attivo flag
    """
    return (
        dp.read_stream("stazioni_aria_raw")
        .filter(upper(trim(col("provincia"))) == "BG")
        .select(
            trim(col("idsensore")).alias("idsensore"),
            trim(col("nomestazione")).alias("nomestazione"),
            trim(col("idstazione")).alias("idstazione"),
            trim(col("comune")).alias("comune"),
            upper(trim(col("provincia"))).alias("provincia"),
            col("lat").cast("double").alias("lat"),
            col("lng").cast("double").alias("lng"),
            col("quota").cast("double").alias("quota"),
            trim(col("nometiposensore")).alias("nometiposensore"),
            trim(col("unitamisura")).alias("unitamisura"),
            to_timestamp(col("datastart")).alias("datastart"),
            to_timestamp(col("datastop")).alias("datastop"),
            when(col("datastop").isNull(), True).otherwise(False).alias("is_attivo"),
            col("_ingestion_ts")
        )
    )


# Target Silver streaming table with SCD Type 2
dp.create_streaming_table(
    name=target_stazioni_silver,
    comment="Cleaned and validated air quality monitoring stations in the Province of Bergamo (SCD Type 2)",
    schema=SILVER_STAZIONI_SCHEMA,
    table_properties={
        "quality": "silver",
        "delta.enableChangeDataFeed": "true",
        "pipelines.autoOptimize.zOrderCols": "idsensore,comune"
    }
)

dp.create_auto_cdc_flow(
    target=target_stazioni_silver,
    source="silver_stazioni_aria_clean",
    keys=["idsensore"],
    sequence_by=col("_ingestion_ts"),
    stored_as_scd_type="2"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Silver Measurements: Cleansing, Anomaly Filtering & Auto CDC (SCD2)

# COMMAND ----------

SILVER_RILEVAZIONI_SCHEMA = """
    idsensore STRING COMMENT 'Unique identifier of the ARPA sensor (foreign key to silver stazioni_aria)',
    data TIMESTAMP COMMENT 'Measurement timestamp for hourly reading',
    valore DOUBLE COMMENT 'Cleaned concentration reading in physical range (valore >= 0)',
    stato STRING COMMENT 'Normalized validation status code (VA, NC, NA)',
    idoperatore STRING COMMENT 'Identifier of the validating operator or authority',
    anno INT COMMENT 'Extracted year of observation',
    mese INT COMMENT 'Extracted month of observation (1-12)',
    giorno INT COMMENT 'Extracted day of month (1-31)',
    ora INT COMMENT 'Extracted hour of observation (0-23)',
    giorno_settimana STRING COMMENT 'Short day of week name (Mon, Tue, etc.)',
    is_weekend BOOLEAN COMMENT 'Flag indicating weekend days (Saturday or Sunday)',
    _ingestion_ts TIMESTAMP COMMENT 'Ingestion timestamp propagated from bronze',
    __START_AT TIMESTAMP COMMENT 'Record validity start timestamp managed automatically by Auto CDC (SCD Type 2)',
    __END_AT TIMESTAMP COMMENT 'Record validity end timestamp managed automatically by Auto CDC (SCD Type 2, NULL for current active record)'
"""

@dp.view(
    name="silver_rilevazioni_aria_clean",
    comment="Streaming view cleansing air quality measurements and removing anomaly values"
)
@dp.expect_or_drop("valid_keys", "idsensore IS NOT NULL AND data IS NOT NULL")
@dp.expect_or_drop("non_negative_physical_value", "valore IS NOT NULL AND valore >= 0.0")
@dp.expect("plausible_pollution_range", "valore <= 1000.0")
def silver_rilevazioni_aria_clean():
    """
    Cleans raw measurements stream:
    - Filters out negative sentinel values (-999) and unphysical values
    - Normalizes status flags
    - Enriches with temporal calendar dimensions for downstream aggregation
    """
    return (
        dp.read_stream("rilevazioni_aria_raw")
        .filter(col("valore").isNotNull() & (col("valore") >= 0.0))
        .select(
            trim(col("idsensore")).alias("idsensore"),
            col("data"),
            col("valore").cast("double").alias("valore"),
            upper(trim(col("stato"))).alias("stato"),
            trim(col("idoperatore")).alias("idoperatore"),
            year(col("data")).alias("anno"),
            month(col("data")).alias("mese"),
            dayofmonth(col("data")).alias("giorno"),
            hour(col("data")).alias("ora"),
            date_format(col("data"), "E").alias("giorno_settimana"),
            when(dayofweek(col("data")).isin(1, 7), True).otherwise(False).alias("is_weekend"),
            col("_ingestion_ts")
        )
    )


# Target Silver streaming table with SCD Type 2
dp.create_streaming_table(
    name=target_rilevazioni_silver,
    comment="Cleaned, validated, and temporally enriched air quality measurements in Bergamo (SCD Type 2)",
    schema=SILVER_RILEVAZIONI_SCHEMA,
    table_properties={
        "quality": "silver",
        "delta.enableChangeDataFeed": "true",
        "pipelines.autoOptimize.zOrderCols": "idsensore,data"
    }
)

dp.create_auto_cdc_flow(
    target=target_rilevazioni_silver,
    source="silver_rilevazioni_aria_clean",
    keys=["idsensore", "data"],
    sequence_by=col("_ingestion_ts"),
    stored_as_scd_type="2"
)
