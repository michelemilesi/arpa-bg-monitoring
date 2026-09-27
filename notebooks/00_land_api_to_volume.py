# Databricks notebook source
# MAGIC %md
# MAGIC # 00 - API Extraction & Landing into Unity Catalog Volume
# MAGIC 
# MAGIC This notebook handles extracting data from the **ARPA Lombardia** Open Data SODA services (`dati.lombardia.it`):
# MAGIC 1. Initializes the Medallion schemas and provisions the `landing` Unity Catalog Volume.
# MAGIC 2. Downloads air quality station and sensor metadata (`ib47-atvt`) with throttling handling (without token).
# MAGIC 3. Downloads sensor measurement batches (`nicp-bhqi`) for sensors located in the Province of Bergamo (`BG`).
# MAGIC 4. Lands raw JSON files into `/Volumes/<catalog>/<bronze_schema>/landing/`, ready for streaming ingestion via Auto Loader and Auto CDC in the Spark Declarative Pipeline.

# COMMAND ----------

import json
import os
import sys
import uuid
from datetime import datetime

# Add project root to PYTHONPATH
current_dir = os.path.dirname(os.path.abspath("__file__"))
parent_dir = os.path.abspath(os.path.join(current_dir, ".."))
for p in [current_dir, parent_dir]:
    if p not in sys.path:
        sys.path.insert(0, p)

from src.arpa_client import ArpaSocrataClient

# Databricks widgets and runtime parameters
dbutils.widgets.text("catalog", "arpa_bg", "1. Target Catalog")
dbutils.widgets.text("bronze_schema", "dev_bronze", "2. Bronze Schema")
dbutils.widgets.text("silver_schema", "dev_silver", "3. Silver Schema")
dbutils.widgets.text("gold_schema", "dev_gold", "4. Gold Schema")
dbutils.widgets.text("filter_province", "BG", "5. Province Filter")
dbutils.widgets.text("start_date", "2024-01-01T00:00:00.000", "6. Start Date")
dbutils.widgets.text("page_size", "50000", "7. SODA Page Size")
dbutils.widgets.dropdown("include_historical", "false", ["false", "true"], "8. Include Historical (pre-2026)")

catalog = dbutils.widgets.get("catalog").strip()
bronze_schema = dbutils.widgets.get("bronze_schema").strip()
silver_schema = dbutils.widgets.get("silver_schema").strip()
gold_schema = dbutils.widgets.get("gold_schema").strip()
filter_province = dbutils.widgets.get("filter_province").strip()
start_date = dbutils.widgets.get("start_date").strip()
page_size = int(dbutils.widgets.get("page_size").strip())
include_historical = dbutils.widgets.get("include_historical").strip().lower() in ("true", "1", "yes")

print(f"Runtime parameters: province={filter_province}, start_date={start_date}, include_historical={include_historical}")

batch_id = str(uuid.uuid4())
run_ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1. Unity Catalog Schemas and Landing Volume Provisioning

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{bronze_schema}` COMMENT 'Bronze Layer: Raw data with SCD2 Auto CDC'")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{silver_schema}` COMMENT 'Silver Layer: Cleaned and enriched air quality data with SCD2'")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{gold_schema}` COMMENT 'Gold Layer: Aggregated metrics with SCD1'")

# Provision the landing Volume for raw JSON landing files
spark.sql(f"CREATE VOLUME IF NOT EXISTS `{catalog}`.`{bronze_schema}`.`landing` COMMENT 'Landing area for raw JSON files extracted from ARPA SODA API'")

landing_volume_path = f"/Volumes/{catalog}/{bronze_schema}/landing"
stazioni_landing_dir = f"{landing_volume_path}/stazioni"
rilevazioni_landing_dir = f"{landing_volume_path}/rilevazioni"
anagrafica_stime_landing_dir = f"{landing_volume_path}/anagrafica_stime"
stime_landing_dir = f"{landing_volume_path}/stime"

# Ensure landing subdirectories exist
for path in [stazioni_landing_dir, rilevazioni_landing_dir, anagrafica_stime_landing_dir, stime_landing_dir]:
    try:
        dbutils.fs.mkdirs(path)
    except Exception as e:
        print(f"Directory {path} already exists or ready: {e}")

print(f"Landing volume configured at: {landing_volume_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. Station Metadata Extraction & Landing (`ib47-atvt`)

# COMMAND ----------

client = ArpaSocrataClient(
    max_retries=5,
    base_delay=2.0,
    polite_delay=0.5,
    timeout=60,
)

print(f"Downloading station metadata for province: {filter_province}...")
stations_data = client.get_stations(province=filter_province, limit=50000)
print(f"Downloaded {len(stations_data)} station/sensor records.")

if not stations_data:
    raise ValueError(f"No station data returned for province {filter_province}")

# Land raw JSON file into the Volume for Auto Loader
stazioni_filename = f"{stazioni_landing_dir}/stazioni_batch_{run_ts}_{batch_id}.json"
dbutils.fs.put(stazioni_filename, json.dumps(stations_data), overwrite=True)
print(f"Station metadata file landed at: {stazioni_filename}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. Hourly Sensor Measurement Extraction & Landing (`g2hp-ar79` [2018-2025] & `nicp-bhqi` [2026+])

# COMMAND ----------

# Extract list of sensor IDs for Bergamo
sensor_ids = [str(item["idsensore"]) for item in stations_data if "idsensore" in item]
print(f"Sensors identified for Bergamo province: {len(sensor_ids)}")

meas_datasets = client.get_measurement_datasets(start_date=start_date, include_historical=include_historical)
print(f"Target physical measurement datasets for start_date '{start_date}' (include_historical={include_historical}): {meas_datasets}")

meas_chunk_idx = 0
total_measurements = 0

for ds in meas_datasets:
    print(f"Starting batched measurement extraction from dataset '{ds}' with throttling management...")
    for page in client.iter_measurements_for_sensors(
        sensor_ids=sensor_ids,
        start_date=start_date if start_date else None,
        chunk_size_sensors=40,
        page_size=page_size,
        dataset_id=ds,
    ):
        if not page:
            continue

        meas_chunk_idx += 1
        total_measurements += len(page)

        meas_filename = f"{rilevazioni_landing_dir}/rilevazioni_{ds}_batch_{run_ts}_{meas_chunk_idx}_{batch_id}.json"
        dbutils.fs.put(meas_filename, json.dumps(page), overwrite=True)
        print(f"[{ds}] Chunk {meas_chunk_idx}: landed {len(page)} measurements to {meas_filename}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4. Municipal Estimates Sensor Registry Extraction & Landing (`5rep-i3mj`)

# COMMAND ----------

print(f"Downloading municipal estimates sensor registry for province: {filter_province}...")
estimates_registry_data = client.get_estimates_registry(province=filter_province, limit=50000)
print(f"Downloaded {len(estimates_registry_data)} municipal estimate sensor records.")

if estimates_registry_data:
    est_registry_filename = f"{anagrafica_stime_landing_dir}/anagrafica_stime_batch_{run_ts}_{batch_id}.json"
    dbutils.fs.put(est_registry_filename, json.dumps(estimates_registry_data), overwrite=True)
    print(f"Municipal estimates registry landed at: {est_registry_filename}")
else:
    print(f"Warning: No estimates registry records returned for province {filter_province}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5. Municipal Air Quality Estimates Data Extraction & Landing (`qyg8-q6gd` [2024], `2vr2-r6un` [2025], `ysm5-jwrn` [2026+])

# COMMAND ----------

# Extract list of sensor IDs for municipal estimates in Bergamo
estimates_sensor_ids = [str(item["idsensore"]) for item in estimates_registry_data if "idsensore" in item]
print(f"Municipal estimate sensors identified for {filter_province}: {len(estimates_sensor_ids)}")

est_datasets = client.get_estimate_datasets(start_date=start_date, include_historical=include_historical)
print(f"Target municipal estimate datasets for start_date '{start_date}' (include_historical={include_historical}): {est_datasets}")

est_chunk_idx = 0
total_estimates = 0

for ds in est_datasets:
    print(f"Starting batched municipal estimates extraction from dataset '{ds}' with throttling management...")
    for page in client.iter_measurements_for_sensors(
        sensor_ids=estimates_sensor_ids,
        start_date=start_date if start_date else None,
        chunk_size_sensors=40,
        page_size=page_size,
        dataset_id=ds,
    ):
        if not page:
            continue

        est_chunk_idx += 1
        total_estimates += len(page)

        est_filename = f"{stime_landing_dir}/stime_{ds}_batch_{run_ts}_{est_chunk_idx}_{batch_id}.json"
        dbutils.fs.put(est_filename, json.dumps(page), overwrite=True)
        print(f"[{ds}] Chunk {est_chunk_idx}: landed {len(page)} municipal estimates to {est_filename}")

print(f"\nLanding completed successfully:")
print(f"- Stations (ib47-atvt):              1 JSON file ({len(stations_data)} records)")
print(f"- Measurements ({', '.join(meas_datasets)}): {meas_chunk_idx} JSON files ({total_measurements} total records)")
print(f"- Estimates Registry (5rep-i3mj):    1 JSON file ({len(estimates_registry_data)} records)")
print(f"- Estimates Data ({', '.join(est_datasets)}): {est_chunk_idx} JSON files ({total_estimates} total records)")
print(f"Raw landing files ready for Spark Declarative Pipeline (DLT) at: {landing_volume_path}")

