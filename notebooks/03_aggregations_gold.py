# Databricks notebook source
# MAGIC %md
# MAGIC # 03 - Spark Declarative Pipeline (Gold Analytics & KPIs - SCD1)
# MAGIC 
# MAGIC This notebook implements the **Gold Layer** of the Spark Declarative Pipeline using `pyspark.pipelines`:
# MAGIC 1. Reads current active records (`__END_AT IS NULL`) from Silver SCD2 tables (`stazioni_aria`, `rilevazioni_aria`, `anagrafica_stime`, `stime_comunali`).
# MAGIC 2. Implements **SCD1** aggregate datasets (current snapshot state updated in-place via Declarative Pipeline Materialized Views).
# MAGIC 3. Computes:
# MAGIC    - **`daily_metrics`**: Daily average, minimum, maximum, and 24-hour reading completeness percentage by municipality and pollutant.
# MAGIC    - **`exceedances`**: Legal threshold exceedances based on Italian regulatory standards (D.Lgs. 155/2010) for PM10, PM2.5, NO2, and Ozone.
# MAGIC    - **`station_summary`**: High-level station registry overview with geographic coordinates, active sensors, and temporal range.
# MAGIC    - **`comuni`**: Comprehensive catalog of municipalities in the Province of Bergamo from municipal estimates registry (`anagrafica_stime`).
# MAGIC    - **`inquinanti`**: Catalog of monitored and estimated air pollutants with coverage and legal thresholds.
# MAGIC    - **`stime_comunali`**: Enriched municipal air quality daily estimates joining active estimates with sensor and municipality metadata.
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
target_comuni = f"{gold_schema}.comuni"
target_inquinanti = f"{gold_schema}.inquinanti"
target_stime_comunali = f"{gold_schema}.stime_comunali"

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

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Gold Municipalities Catalog (`comuni`) from Municipal Estimates Registry

# COMMAND ----------

COMUNI_SCHEMA = """
    comune STRING COMMENT 'Municipality name in the Province of Bergamo',
    provincia STRING COMMENT 'Province code (strictly BG)',
    num_sensori_totali LONG COMMENT 'Total number of municipal air quality estimate sensors',
    num_sensori_attivi LONG COMMENT 'Number of currently active estimate sensors in the municipality',
    inquinanti_stimati STRING COMMENT 'Comma-separated list of estimated pollutants in the municipality',
    data_inizio_attivita TIMESTAMP COMMENT 'Earliest start date of municipal air quality estimates',
    data_fine_attivita TIMESTAMP COMMENT 'Latest deactivation date of estimate sensors (NULL if currently active)',
    _updated_at TIMESTAMP COMMENT 'Deterministic timestamp of latest source ingestion (_ingestion_ts)'
"""

@dp.table(
    name=target_comuni,
    comment="Catalog of municipalities in the Province of Bergamo derived from municipal estimates registry (SCD1)",
    schema=COMUNI_SCHEMA,
    table_properties={
        "quality": "gold",
        "pipelines.autoOptimize.zOrderCols": "comune"
    }
)
def comuni():
    """
    Produces the distinct list of municipalities in Bergamo from active anagrafica_stime records,
    enriching each municipality with sensor counts, estimated pollutants, and activity window.
    """
    silver_stime = (
        dp.read(f"{silver_schema}.anagrafica_stime")
        .filter(col("__END_AT").isNull())
    )

    return (
        silver_stime
        .groupBy(
            col("comune"),
            col("provincia")
        )
        .agg(
            countDistinct("idsensore").alias("num_sensori_totali"),
            countDistinct(when(col("is_attivo") == True, col("idsensore"))).alias("num_sensori_attivi"),
            concat_ws(", ", collect_set("nometiposensore")).alias("inquinanti_stimati"),
            spark_min("datastart").alias("data_inizio_attivita"),
            spark_max("datastop").alias("data_fine_attivita"),
            spark_max("_ingestion_ts").alias("_updated_at")
        )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Gold Monitored & Estimated Pollutants Catalog (`inquinanti`)

# COMMAND ----------

INQUINANTI_SCHEMA = """
    nometiposensore STRING COMMENT 'Standardized name of the pollutant parameter (e.g., PM10, PM2.5, Biossido di Azoto, Ozono)',
    unitamisura STRING COMMENT 'Unit of measurement for pollutant concentration (e.g., µg/m³, mg/m³)',
    ha_misure_stazioni BOOLEAN COMMENT 'Flag indicating if the pollutant is monitored by physical stations',
    ha_stime_comunali BOOLEAN COMMENT 'Flag indicating if the pollutant has municipal model-based estimates',
    num_sensori_totali LONG COMMENT 'Total number of active sensors (physical + virtual estimates)',
    num_sensori_stazioni LONG COMMENT 'Number of physical monitoring station sensors',
    num_sensori_stime LONG COMMENT 'Number of municipal estimate virtual sensors',
    num_comuni_coperti LONG COMMENT 'Number of distinct municipalities covered in Bergamo province',
    soglia_riferimento_legge STRING COMMENT 'Regulatory reference threshold limit under Italian D.Lgs. 155/2010',
    _updated_at TIMESTAMP COMMENT 'Deterministic timestamp of latest source ingestion (_ingestion_ts)'
"""

@dp.table(
    name=target_inquinanti,
    comment="Catalog of monitored and estimated air quality pollutants in Bergamo with coverage and regulatory limits (SCD1)",
    schema=INQUINANTI_SCHEMA,
    table_properties={
        "quality": "gold",
        "pipelines.autoOptimize.zOrderCols": "nometiposensore"
    }
)
def inquinanti():
    """
    Produces the consolidated list of pollutants monitored or estimated across Bergamo province,
    aggregating metadata from active physical stations (stazioni_aria) and municipal estimates (anagrafica_stime).
    """
    stazioni = (
        dp.read(f"{silver_schema}.stazioni_aria")
        .filter(col("__END_AT").isNull())
        .select(
            col("nometiposensore"),
            col("unitamisura"),
            col("comune"),
            col("idsensore"),
            lit("stazione").alias("fonte"),
            col("_ingestion_ts")
        )
    )

    stime = (
        dp.read(f"{silver_schema}.anagrafica_stime")
        .filter(col("__END_AT").isNull())
        .select(
            col("nometiposensore"),
            col("unitamisura"),
            col("comune"),
            col("idsensore"),
            lit("stima").alias("fonte"),
            col("_ingestion_ts")
        )
    )

    combined = stazioni.unionByName(stime)

    return (
        combined
        .groupBy(
            col("nometiposensore"),
            col("unitamisura")
        )
        .agg(
            countDistinct("idsensore").alias("num_sensori_totali"),
            countDistinct(when(col("fonte") == "stazione", col("idsensore"))).alias("num_sensori_stazioni"),
            countDistinct(when(col("fonte") == "stima", col("idsensore"))).alias("num_sensori_stime"),
            countDistinct("comune").alias("num_comuni_coperti"),
            spark_max(when(col("fonte") == "stazione", lit(True)).otherwise(lit(False))).alias("ha_misure_stazioni"),
            spark_max(when(col("fonte") == "stima", lit(True)).otherwise(lit(False))).alias("ha_stime_comunali"),
            spark_max("_ingestion_ts").alias("_updated_at")
        )
        .withColumn(
            "soglia_riferimento_legge",
            when(col("nometiposensore").rlike("(?i)PM10"), lit("D.Lgs. 155/2010: Media giornaliera max 50 µg/m³ (max 35 gg/anno)"))
            .when(col("nometiposensore").rlike("(?i)PM2\\.5"), lit("D.Lgs. 155/2010: Media annua 25 µg/m³ (WHO rif. 25 µg/m³ giornaliera)"))
            .when(col("nometiposensore").rlike("(?i)Biossido di Azoto|NO2"), lit("D.Lgs. 155/2010: Soglia oraria max 200 µg/m³"))
            .when(col("nometiposensore").rlike("(?i)Ozono|O3"), lit("D.Lgs. 155/2010: Media 8h max 120 µg/m³"))
            .when(col("nometiposensore").rlike("(?i)Monossido di Carbonio|CO"), lit("D.Lgs. 155/2010: Max 8h mobile 10 mg/m³"))
            .when(col("nometiposensore").rlike("(?i)Biossido di Zolfo|SO2"), lit("D.Lgs. 155/2010: Media oraria max 350 µg/m³"))
            .when(col("nometiposensore").rlike("(?i)Benzene|C6H6"), lit("D.Lgs. 155/2010: Media annua 5 µg/m³"))
            .otherwise(lit("Nessuna soglia tabellare specifica"))
        )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Gold Enriched Municipal Air Quality Estimates (`stime_comunali`)

# COMMAND ----------

STIME_COMUNALI_SCHEMA = """
    nometiposensore STRING COMMENT 'Normalized pollutant name for municipal estimates (e.g., PM10, PM2.5, NO2, Ozono)',
    comune STRING COMMENT 'Municipality name in the Province of Bergamo',
    valore DOUBLE COMMENT 'Model-estimated air quality concentration reading',
    unitamisura STRING COMMENT 'Unit of measurement for pollutant concentration (e.g., µg/m³)',
    data TIMESTAMP COMMENT 'Observation timestamp of the municipal estimate',
    anno INT COMMENT 'Extracted year of observation',
    mese INT COMMENT 'Extracted month of observation (1-12)',
    giorno INT COMMENT 'Extracted day of month (1-31)',
    giorno_settimana STRING COMMENT 'Short day of week name (Mon, Tue, etc.)',
    is_weekend BOOLEAN COMMENT 'Flag indicating weekend days (Saturday or Sunday)',
    _updated_at TIMESTAMP COMMENT 'Deterministic timestamp of latest source ingestion (_ingestion_ts) contributing to this record'
"""

@dp.table(
    name=target_stime_comunali,
    comment="Enriched municipal air quality daily estimates with municipality and pollutant metadata in Bergamo (SCD1)",
    schema=STIME_COMUNALI_SCHEMA,
    table_properties={
        "quality": "gold",
        "pipelines.autoOptimize.zOrderCols": "comune,nometiposensore,data"
    }
)
def stime_comunali():
    """
    Produces enriched municipal air quality estimates by joining active municipal estimates
    with active municipal estimates sensor registry metadata:
    - a.nometiposensore
    - a.comune
    - sc.valore
    - a.unitamisura
    - sc.data
    - sc.anno
    - sc.mese
    - sc.giorno
    - sc.giorno_settimana
    - sc.is_weekend
    - _updated_at (deterministic latest _ingestion_ts)
    Filters current versions (__END_AT is null) to represent the current state (SCD1).
    """
    sc = dp.read(f"{silver_schema}.stime_comunali").filter(col("__END_AT").isNull()).alias("sc")
    a = dp.read(f"{silver_schema}.anagrafica_stime").filter(col("__END_AT").isNull()).alias("a")

    return (
        sc.join(a, on="idsensore", how="inner")
        .select(
            col("a.nometiposensore"),
            col("a.comune"),
            col("sc.valore"),
            col("a.unitamisura"),
            col("sc.data"),
            col("sc.anno"),
            col("sc.mese"),
            col("sc.giorno"),
            col("sc.giorno_settimana"),
            col("sc.is_weekend"),
            coalesce(
                greatest(col("sc._ingestion_ts"), col("a._ingestion_ts")),
                col("sc._ingestion_ts"),
                col("a._ingestion_ts")
            ).alias("_updated_at")
        )
    )
