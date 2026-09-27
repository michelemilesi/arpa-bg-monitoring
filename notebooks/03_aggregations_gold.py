# Databricks notebook source
# MAGIC %md
# MAGIC # 03 - Spark Declarative Pipeline (Gold Analytics & KPIs - SCD1)
# MAGIC 
# MAGIC This notebook implements the **Gold Layer** of the Spark Declarative Pipeline using `pyspark.pipelines`:
# MAGIC 1. Reads current active records (`__END_AT IS NULL`) from the Silver SCD2 tables (`stazioni_aria`, `rilevazioni_aria`).
# MAGIC 2. Implements **SCD1** aggregate datasets (current snapshot state updated in-place via Declarative Pipeline Materialized Views).
# MAGIC 3. Computes:
# MAGIC    - **`daily_metrics`**: Daily average, minimum, maximum, and 24-hour reading completeness percentage by municipality and pollutant.
# MAGIC    - **`exceedances`**: Legal threshold exceedances based on Italian regulatory standards (D.Lgs. 155/2010) for PM10, PM2.5, NO2, and Ozone.
# MAGIC    - **`station_summary`**: High-level station registry overview with geographic coordinates, active sensors, and temporal range.
# MAGIC 4. Clean table names without layer prefixes within the Gold schema (`gold_schema`).
# MAGIC 5. Explicit schemas and full English comments on all tables and columns.

# COMMAND ----------

from pyspark import pipelines as dp
from pyspark.sql.functions import (
    avg,
    coalesce,
    col,
    concat_ws,
    collect_set,
    count,
    countDistinct,
    greatest,
    lit,
    max as spark_max,
    min as spark_min,
    round as spark_round,
    to_date,
    when,
)

# Configuration for multi-schema resolution
silver_schema = spark.conf.get("silver_schema", "dev_silver")
gold_schema = spark.conf.get("gold_schema", "dev_gold")

target_daily_metrics = f"{gold_schema}.daily_metrics"
target_exceedances = f"{gold_schema}.exceedances"
target_station_summary = f"{gold_schema}.station_summary"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Gold Daily Metrics (SCD1)

# COMMAND ----------

DAILY_METRICS_SCHEMA = """
    idstazione STRING COMMENT 'Unique identifier of the ARPA monitoring station',
    nomestazione STRING COMMENT 'Monitoring station name in Bergamo province',
    comune STRING COMMENT 'Municipality where the station is located',
    idsensore STRING COMMENT 'Unique identifier of the specific pollutant sensor',
    nometiposensore STRING COMMENT 'Monitored pollutant type (e.g., PM10, NO2, Ozono, PM2.5)',
    unitamisura STRING COMMENT 'Unit of measurement for the pollutant concentration',
    data_giorno DATE COMMENT 'Date of observation (daily granularity)',
    anno INT COMMENT 'Year of observation',
    mese INT COMMENT 'Month of observation',
    media_giornaliera DOUBLE COMMENT 'Daily average pollutant concentration (SCD1)',
    min_giornaliero DOUBLE COMMENT 'Daily minimum recorded concentration (SCD1)',
    max_giornaliero DOUBLE COMMENT 'Daily maximum recorded concentration (SCD1)',
    num_rilevazioni LONG COMMENT 'Number of valid hourly measurements recorded during the day',
    copertura_24h_pct DOUBLE COMMENT 'Percentage of hourly data coverage over 24 hours (completeness)',
    _updated_at TIMESTAMP COMMENT 'Deterministic timestamp of latest source ingestion (_ingestion_ts) contributing to this metric'
"""

@dp.table(
    name=target_daily_metrics,
    comment="Daily air pollution statistics (average, min, max, coverage) per station and pollutant in Bergamo (SCD1)",
    schema=DAILY_METRICS_SCHEMA,
    table_properties={
        "quality": "gold",
        "pipelines.autoOptimize.zOrderCols": "comune,nometiposensore,data_giorno"
    }
)
def daily_metrics():
    """
    Computes daily aggregated air quality metrics from active Silver records.
    Filters current versions (__END_AT is null) to represent the current state (SCD1).
    Derives _updated_at deterministically from the latest _ingestion_ts among joined records.
    """
    # Active current records from Silver
    silver_stazioni = dp.read(f"{silver_schema}.stazioni_aria").filter(col("__END_AT").isNull())
    silver_rilevazioni = dp.read(f"{silver_schema}.rilevazioni_aria").filter(col("__END_AT").isNull())

    joined = (
        silver_rilevazioni.alias("r")
        .join(
            silver_stazioni.alias("s"),
            on="idsensore",
            how="inner"
        )
    )

    return (
        joined
        .groupBy(
            col("s.idstazione"),
            col("s.nomestazione"),
            col("s.comune"),
            col("r.idsensore"),
            col("s.nometiposensore"),
            col("s.unitamisura"),
            to_date(col("r.data")).alias("data_giorno"),
            col("r.anno"),
            col("r.mese")
        )
        .agg(
            spark_round(avg("r.valore"), 2).alias("media_giornaliera"),
            spark_round(spark_min("r.valore"), 2).alias("min_giornaliero"),
            spark_round(spark_max("r.valore"), 2).alias("max_giornaliero"),
            count("r.valore").alias("num_rilevazioni"),
            spark_round((count("r.valore") / 24.0) * 100.0, 1).alias("copertura_24h_pct"),
            spark_max(greatest(col("r._ingestion_ts"), col("s._ingestion_ts"))).alias("_updated_at")
        )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Gold Regulatory Exceedances (SCD1 — D.Lgs. 155/2010 & EU Directives)

# COMMAND ----------

EXCEEDANCES_SCHEMA = """
    idstazione STRING COMMENT 'Unique identifier of the monitoring station',
    nomestazione STRING COMMENT 'Name of the monitoring station',
    comune STRING COMMENT 'Municipality in Bergamo province',
    idsensore STRING COMMENT 'Unique identifier of the pollutant sensor',
    nometiposensore STRING COMMENT 'Monitored pollutant (PM10, PM2.5, NO2, Ozono)',
    unitamisura STRING COMMENT 'Unit of measurement (e.g., µg/m³)',
    data_osservazione DATE COMMENT 'Date of the measured exceedance',
    valore_osservato DOUBLE COMMENT 'Measured concentration value (daily average or max hourly)',
    soglia_limite_legge DOUBLE COMMENT 'Regulatory threshold limit concentration according to D.Lgs. 155/2010',
    is_superato BOOLEAN COMMENT 'Flag indicating if the measured concentration exceeded the legal threshold',
    delta_superamento DOUBLE COMMENT 'Difference between observed concentration and legal threshold',
    riferimento_normativo STRING COMMENT 'Legal regulatory citation reference (Italian D.Lgs. 155/2010 / EU Directive)',
    _updated_at TIMESTAMP COMMENT 'Deterministic timestamp of latest source ingestion (_ingestion_ts) inherited from daily_metrics'
"""

@dp.table(
    name=target_exceedances,
    comment="Daily tracking of air pollution legal threshold exceedances under Italian D.Lgs. 155/2010 (SCD1)",
    schema=EXCEEDANCES_SCHEMA,
    table_properties={
        "quality": "gold",
        "pipelines.autoOptimize.zOrderCols": "nometiposensore,data_osservazione,is_superato"
    }
)
def exceedances():
    """
    Evaluates regulatory compliance against Italian D.Lgs. 155/2010:
    - PM10: Daily average threshold of 50 µg/m³ (maximum 35 exceedance days allowed per year)
    - PM2.5: Daily reference indicative threshold of 25 µg/m³
    - NO2: Maximum hourly peak threshold of 200 µg/m³
    - Ozono (O3): Maximum daily threshold of 120 µg/m³
    Propagates deterministic _updated_at from daily_metrics.
    """
    daily = dp.read(target_daily_metrics)

    # Threshold mappings according to D.Lgs. 155/2010
    with_limits = (
        daily
        .withColumn(
            "soglia_limite_legge",
            when(col("nometiposensore").rlike("(?i)PM10"), lit(50.0))
            .when(col("nometiposensore").rlike("(?i)PM2\\.5"), lit(25.0))
            .when(col("nometiposensore").rlike("(?i)Biossido di Azoto|NO2"), lit(200.0))
            .when(col("nometiposensore").rlike("(?i)Ozono|O3"), lit(120.0))
            .otherwise(lit(None).cast("double"))
        )
        .withColumn(
            "riferimento_normativo",
            when(col("nometiposensore").rlike("(?i)PM10"), lit("D.Lgs. 155/2010 - Media giornaliera max 50 µg/m³ (max 35 giorni/anno)"))
            .when(col("nometiposensore").rlike("(?i)PM2\\.5"), lit("D.Lgs. 155/2010 / Linee Guida UE - Media giornaliera di riferimento 25 µg/m³"))
            .when(col("nometiposensore").rlike("(?i)Biossido di Azoto|NO2"), lit("D.Lgs. 155/2010 - Soglia oraria max 200 µg/m³"))
            .when(col("nometiposensore").rlike("(?i)Ozono|O3"), lit("D.Lgs. 155/2010 - Soglia max giornaliera 120 µg/m³"))
            .otherwise(lit("Nessuna soglia applicabile"))
        )
        .filter(col("soglia_limite_legge").isNotNull())
    )

    # Determine observed metric depending on pollutant rule
    evaluated = (
        with_limits
        .withColumn(
            "valore_osservato",
            when(col("nometiposensore").rlike("(?i)Biossido di Azoto|NO2"), col("max_giornaliero"))
            .otherwise(col("media_giornaliera"))
        )
        .withColumn("is_superato", col("valore_osservato") > col("soglia_limite_legge"))
        .withColumn("delta_superamento", spark_round(col("valore_osservato") - col("soglia_limite_legge"), 2))
        .select(
            col("idstazione"),
            col("nomestazione"),
            col("comune"),
            col("idsensore"),
            col("nometiposensore"),
            col("unitamisura"),
            col("data_giorno").alias("data_osservazione"),
            col("valore_osservato"),
            col("soglia_limite_legge"),
            col("is_superato"),
            col("delta_superamento"),
            col("riferimento_normativo"),
            col("_updated_at")
        )
    )

    return evaluated

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Gold Monitoring Station Summary (SCD1)

# COMMAND ----------

STATION_SUMMARY_SCHEMA = """
    idstazione STRING COMMENT 'Unique identifier of the monitoring station',
    nomestazione STRING COMMENT 'Name of the monitoring station',
    comune STRING COMMENT 'Municipality where the station is located in Bergamo province',
    quota DOUBLE COMMENT 'Station elevation in meters above sea level',
    lat DOUBLE COMMENT 'Latitude coordinate (WGS84)',
    lng DOUBLE COMMENT 'Longitude coordinate (WGS84)',
    num_sensori_attivi LONG COMMENT 'Count of distinct currently active sensors installed at the station',
    inquinanti_monitorati STRING COMMENT 'Comma-separated list of pollutants measured at this station',
    data_inizio_rilevazioni TIMESTAMP COMMENT 'Earliest measurement timestamp recorded at this station',
    data_fine_rilevazioni TIMESTAMP COMMENT 'Latest measurement timestamp recorded at this station',
    totale_misure_registrate LONG COMMENT 'Total count of valid sensor readings recorded',
    _updated_at TIMESTAMP COMMENT 'Deterministic timestamp of latest source ingestion (_ingestion_ts) contributing to this summary'
"""

@dp.table(
    name=target_station_summary,
    comment="Station-level geographic and sensor inventory overview across Bergamo province (SCD1)",
    schema=STATION_SUMMARY_SCHEMA,
    table_properties={
        "quality": "gold",
        "pipelines.autoOptimize.zOrderCols": "comune,nomestazione"
    }
)
def station_summary():
    """
    Produces station-level overview:
    - Geographic position (latitude, longitude, altitude)
    - Active sensor count and list of monitored pollutants
    - Temporal period of measurement coverage
    - Total measurement volume
    Derives _updated_at deterministically from the latest _ingestion_ts among joined records.
    """
    silver_stazioni = dp.read(f"{silver_schema}.stazioni_aria").filter(col("__END_AT").isNull())
    silver_rilevazioni = dp.read(f"{silver_schema}.rilevazioni_aria").filter(col("__END_AT").isNull())

    joined = (
        silver_stazioni.alias("s")
        .join(
            silver_rilevazioni.alias("r"),
            on="idsensore",
            how="left"
        )
    )

    return (
        joined
        .groupBy(
            col("s.idstazione"),
            col("s.nomestazione"),
            col("s.comune"),
            col("s.quota"),
            col("s.lat"),
            col("s.lng")
        )
        .agg(
            countDistinct("s.idsensore").alias("num_sensori_attivi"),
            concat_ws(", ", collect_set("s.nometiposensore")).alias("inquinanti_monitorati"),
            spark_min("r.data").alias("data_inizio_rilevazioni"),
            spark_max("r.data").alias("data_fine_rilevazioni"),
            count("r.valore").alias("totale_misure_registrate"),
            coalesce(
                spark_max(greatest(col("s._ingestion_ts"), col("r._ingestion_ts"))),
                spark_max(col("s._ingestion_ts"))
            ).alias("_updated_at")
        )
    )
