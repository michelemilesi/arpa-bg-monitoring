# Databricks notebook source
# MAGIC %md
# MAGIC # 01 - Spark Declarative Pipeline (Bronze Ingestion & Auto CDC)
# MAGIC 
# MAGIC This notebook implements a **Spark Declarative Pipeline (Lakeflow Pipelines)** using the standard `pyspark.pipelines` package:
# MAGIC 1. Incremental streaming ingestion using **Auto Loader (`cloudFiles`)** from raw JSON files landed in the Unity Catalog Volume.
# MAGIC 2. Data quality enforcement using **Pipeline Expectations** (`@dp.expect_or_drop`).
# MAGIC 3. Explicit schema definitions with comprehensive table and column comments in English.
# MAGIC 4. Fully automated **SCD Type 2 (SCD2)** history tracking via **Auto CDC (`dp.create_auto_cdc_flow(stored_as_scd_type="2")`)**.
# MAGIC 5. Clean table names without layer prefixes (`stazioni_aria`, `rilevazioni_aria`) within the target schema (`bronze_schema`).

# COMMAND ----------

from pyspark import pipelines as dp
from pyspark.sql.functions import col, current_timestamp, to_timestamp

# Base landing volume and schema configuration
landing_volume = spark.conf.get(
    "landing_volume",
    "/Volumes/arpa_bg/dev_bronze/landing"
)
bronze_schema = spark.conf.get("bronze_schema", "dev_bronze")
stazioni_landing_path = f"{landing_volume}/stazioni"
rilevazioni_landing_path = f"{landing_volume}/rilevazioni"
target_stazioni_bronze = f"{bronze_schema}.stazioni_aria"
target_rilevazioni_bronze = f"{bronze_schema}.rilevazioni_aria"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Monitoring Stations and Sensors: Streaming Ingestion & Auto CDC (SCD2)

# COMMAND ----------

# Explicit schema definition with column comments for stazioni_aria
STAZIONI_SCHEMA = """
    idsensore STRING COMMENT 'Unique identifier of the ARPA Lombardia air quality sensor (business primary key)',
    nomestazione STRING COMMENT 'Name of the monitoring station (e.g., Bergamo - Via Meucci, Treviglio)',
    idstazione STRING COMMENT 'Unique identifier of the ARPA monitoring station',
    comune STRING COMMENT 'Municipality in the Province of Bergamo where the station is located',
    provincia STRING COMMENT 'Province code (e.g., BG for Bergamo)',
    lat DOUBLE COMMENT 'Geographic latitude coordinate in decimal degrees (WGS84)',
    lng DOUBLE COMMENT 'Geographic longitude coordinate in decimal degrees (WGS84)',
    quota DOUBLE COMMENT 'Altitude of the monitoring station in meters above sea level',
    nometiposensore STRING COMMENT 'Monitored pollutant parameter (e.g., Particulate PM10, Nitrogen Dioxide NO2, Ozone O3, PM2.5)',
    unitamisura STRING COMMENT 'Unit of measurement for pollutant concentration (e.g., µg/m³, mg/m³)',
    datastart STRING COMMENT 'Sensor activation start date (ISO 8601 string from ARPA)',
    datastop STRING COMMENT 'Sensor deactivation or end date (NULL if currently active)',
    storico STRING COMMENT 'Historical archive flag indicator (S for historical/archived, N for active)',
    _ingestion_ts TIMESTAMP COMMENT 'Technical ingestion timestamp from the ARPA Open Data service',
    __START_AT TIMESTAMP COMMENT 'Record validity start timestamp managed automatically by Auto CDC (SCD Type 2)',
    __END_AT TIMESTAMP COMMENT 'Record validity end timestamp managed automatically by Auto CDC (SCD Type 2, NULL for current active record)'
"""

@dp.view(
    name="stazioni_aria_raw",
    comment="Streaming view using Auto Loader for raw station metadata ingestion from JSON files"
)
@dp.expect_or_drop("valid_idsensore", "idsensore IS NOT NULL")
def stazioni_aria_raw():
    """
    Incrementally reads raw station metadata JSON files from the Unity Catalog Volume,
    normalizes data types, and appends the ingestion timestamp for Auto CDC sequencing.
    """
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaHints", "datastart string, datastop string, lat double, lng double, quota double")
        .load(stazioni_landing_path)
        .select(
            col("idsensore").cast("string").alias("idsensore"),
            col("nomestazione").cast("string").alias("nomestazione"),
            col("idstazione").cast("string").alias("idstazione"),
            col("comune").cast("string").alias("comune"),
            col("provincia").cast("string").alias("provincia"),
            col("lat").cast("double").alias("lat"),
            col("lng").cast("double").alias("lng"),
            col("quota").cast("double").alias("quota"),
            col("nometiposensore").cast("string").alias("nometiposensore"),
            col("unitamisura").cast("string").alias("unitamisura"),
            col("datastart").cast("string").alias("datastart"),
            col("datastop").cast("string").alias("datastop"),
            col("storico").cast("string").alias("storico"),
            current_timestamp().alias("_ingestion_ts")
        )
    )


# Target streaming table creation with explicit schema and table comment
dp.create_streaming_table(
    name=target_stazioni_bronze,
    comment="Air quality monitoring stations and sensors in the Province of Bergamo (ARPA Lombardia) with historical change tracking (SCD Type 2)",
    schema=STAZIONI_SCHEMA,
    table_properties={
        "quality": "bronze",
        "delta.enableChangeDataFeed": "true",
        "pipelines.autoOptimize.zOrderCols": "idsensore"
    }
)

# Apply Auto CDC with SCD Type 2 history tracking using modern create_auto_cdc_flow
dp.create_auto_cdc_flow(
    target=target_stazioni_bronze,
    source="stazioni_aria_raw",
    keys=["idsensore"],
    sequence_by=col("_ingestion_ts"),
    stored_as_scd_type="2"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Sensor Measurements: Streaming Ingestion & Auto CDC (SCD2)

# COMMAND ----------

# Explicit schema definition with column comments for rilevazioni_aria
RILEVAZIONI_SCHEMA = """
    idsensore STRING COMMENT 'Unique identifier of the ARPA sensor (foreign key to stazioni_aria)',
    data TIMESTAMP COMMENT 'Measurement timestamp for hourly or daily pollutant reading',
    valore DOUBLE COMMENT 'Numeric concentration value recorded by the air quality sensor',
    stato STRING COMMENT 'Validation status flag (VA: Validated, NC: Non-compliant, NA: Not acquired)',
    idoperatore STRING COMMENT 'Identifier of the operator or validating entity',
    _ingestion_ts TIMESTAMP COMMENT 'Technical ingestion timestamp from the ARPA Open Data service',
    __START_AT TIMESTAMP COMMENT 'Record validity start timestamp managed automatically by Auto CDC (SCD Type 2)',
    __END_AT TIMESTAMP COMMENT 'Record validity end timestamp managed automatically by Auto CDC (SCD Type 2, NULL for current active record)'
"""

@dp.view(
    name="rilevazioni_aria_raw",
    comment="Streaming view using Auto Loader for raw sensor measurement readings from JSON files"
)
@dp.expect_or_drop("valid_idsensore_data", "idsensore IS NOT NULL AND data IS NOT NULL")
def rilevazioni_aria_raw():
    """
    Incrementally reads raw measurement JSON files from the Unity Catalog Volume,
    casts fields to native types (TIMESTAMP, DOUBLE), and appends the ingestion timestamp.
    """
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .load(rilevazioni_landing_path)
        .select(
            col("idsensore").cast("string").alias("idsensore"),
            to_timestamp(col("data")).alias("data"),
            col("valore").cast("double").alias("valore"),
            col("stato").cast("string").alias("stato"),
            col("idoperatore").cast("string").alias("idoperatore"),
            current_timestamp().alias("_ingestion_ts")
        )
    )


# Target streaming table creation with explicit schema and table comment
dp.create_streaming_table(
    name=target_rilevazioni_bronze,
    comment="Hourly and daily air quality sensor measurements in the Province of Bergamo with automated historical correction tracking (SCD Type 2)",
    schema=RILEVAZIONI_SCHEMA,
    table_properties={
        "quality": "bronze",
        "delta.enableChangeDataFeed": "true",
        "pipelines.autoOptimize.zOrderCols": "idsensore,data"
    }
)

# Apply Auto CDC with SCD Type 2 history tracking on composite key (idsensore, data)
dp.create_auto_cdc_flow(
    target=target_rilevazioni_bronze,
    source="rilevazioni_aria_raw",
    keys=["idsensore", "data"],
    sequence_by=col("_ingestion_ts"),
    stored_as_scd_type="2"
)
