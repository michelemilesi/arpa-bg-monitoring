# AGENTS.md — ARPA Lombardia Pollution Monitoring (Province of Bergamo)

This document defines the rules, technical architecture, development conventions, and operational guidelines for any AI agent collaborating on this repository.

---

## 1. Project Objective

The **arpa-bg-monitoring** project aims to:
1. **Acquire** open air quality monitoring data provided by **ARPA Lombardia** via the Regione Lombardia Open Data portal (`dati.lombardia.it`).
2. **Process and filter** the data using **Databricks** (PySpark, Delta Lake, Spark SQL), focusing specifically on the municipalities in the **Province of Bergamo (BG)**.
3. **Analyze and visualize** key air pollution indicators (PM10, PM2.5, NO2, O3, CO, SO2, Benzene) through Databricks notebooks and dashboards, highlighting historical trends, regulatory threshold exceedances, and comparisons across municipalities.

---

## 2. Data Sources & Socrata API (ARPA Lombardia)

All datasets originate from **Open Data Regione Lombardia** hosted on the Socrata platform.

### 2.1 Reference Datasets

| Dataset | Socrata ID | SODA Endpoint | Description |
|---|---|---|---|
| **Air Quality Stations (Metadata)** | `ib47-atvt` | `https://www.dati.lombardia.it/resource/ib47-atvt.json` | Registry of monitoring stations and sensors: coordinates (`lat`, `lng`), municipality, province, measured pollutant, altitude, start/end dates of activity. |
| **Air Quality Sensor Data (Measurements)** | `nicp-bhqi` | `https://www.dati.lombardia.it/resource/nicp-bhqi.json` | Hourly/daily sensor measurements including `IdSensore`, `Data`, `Valore`, `Stato`. |
| **Municipal Air Quality Estimates** | `2vr2-r6un` | `https://www.dati.lombardia.it/resource/2vr2-r6un.json` | Model-based municipal estimates produced by ARPA for municipalities without fixed physical monitoring stations. |

### 2.2 Ingestion Method (SODA API)

- **Protocol**: REST HTTP (SODA - Socrata Open Data API).
- **Supported formats**: JSON or CSV. For high volumes or Spark pipelines, prefer batch JSON or CSV downloads.
- **SoQL Query Parameters**:
  - `$where`: e.g., `provincia = 'BG'` for station metadata, or `IdSensore in (...)` for sensor measurements.
  - `$limit`: for pagination (default Socrata limit: 1,000 records; max supported per request: 50,000).
  - `$offset`: to page through records.
  - `$order`: e.g., `Data DESC` or `IdSensore ASC`.
- **Authentication**: Optional for casual/infrequent access. An **App Token** passed via the `X-App-Token` HTTP header is recommended for automated pipelines to avoid rate limits (retrieve via `dbutils.secrets`).

---

## 3. Geographical Scope & Pollutants (Province of Bergamo)

### 3.1 Geographic Filtering
- All measurement data must be filtered for sensors located within the **Province of Bergamo** (`Provincia == 'BG'`).
- Key monitoring stations across Bergamo province:
  - **Bergamo city**: *Via Meucci*, *Via Garibaldi*, *Borgo Palazzo*.
  - **Province / Plain and valleys**: *Dalmine*, *Osio Sotto*, *Treviglio*, *Filago*, *Casirate d'Adda*, *Sarnico*, etc.

### 3.2 Pollutants and Regulatory Thresholds (Italian D.Lgs. 155/2010 & WHO Guidelines)

| Pollutant | Parameter | Reference Limits / Legal Thresholds | Notes |
|---|---|---|---|
| **PM10** | Particulate matter ≤ 10 µm | **50 µg/m³** daily average (max 35 exceedance days/year); **40 µg/m³** annual average | Critical winter pollutant across the Po Valley |
| **PM2.5** | Particulate matter ≤ 2.5 µm | **25 µg/m³** annual average (WHO recommends 5 µg/m³) | Fine particulate fraction |
| **NO2** | Nitrogen Dioxide | **200 µg/m³** hourly average (max 18 exceedances/year); **40 µg/m³** annual average | Primarily vehicle traffic and heating emissions |
| **O3** | Ozone | **120 µg/m³** max daily 8-hour mean (max 25 days/year); **180 µg/m³** information threshold; **240 µg/m³** alert threshold | Typical summer photochemical pollutant |
| **CO** | Carbon Monoxide | **10 mg/m³** max 8-hour moving average | Combustion monitoring |
| **SO2** | Sulfur Dioxide | **350 µg/m³** hourly average; **125 µg/m³** 24-hour average | Industrial emissions |
| **C6H6** | Benzene | **5 µg/m³** annual average | Volatile organic compound monitoring |

---

## 4. Technical Architecture on Databricks (Medallion Architecture)

Data must be structured according to the **Medallion Architecture** pattern using **Delta Lake** tables:

```
[ARPA Lombardia SODA API]
           │
           ▼
┌───────────────────────┐
│     Bronze Layer      │  Raw data ingested from API (JSON / Delta)
│ (raw_stazioni_aria,   │  Original schema + ingestion metadata: `_ingestion_ts`, `_source`
│  raw_misure_aria)     │
└──────────┬────────────┘
           │
           ▼
┌───────────────────────┐
│     Silver Layer      │  Filtered by Provincia = 'BG', explicit type casting (Timestamp, Double),
│ (silver_stazioni_bg,  │  join stations + measurements, clean invalid values (-999, nulls),
│  silver_rilevazioni_bg)│  enrich with temporal features (year, month, day, hour).
└──────────┬────────────┘
           │
           ▼
┌───────────────────────┐
│      Gold Layer       │  Aggregated analytics tables:
│ (gold_daily_metrics,  │  - Daily and monthly averages per municipality and pollutant
│  gold_exceedances,    │  - Count of legal threshold exceedances (e.g., PM10 > 50 µg/m³)
│  gold_station_summary)│  - Optimized datasets for Lakeview/Databricks SQL dashboards
└───────────────────────┘
```

---

## 5. Repository Structure

The file structure within the repository should adhere to this layout:

```
arpa-bg-monitoring/
├── README.md                           # Project overview and quickstart guide
├── AGENTS.md                           # AI agent rules, instructions, and architecture
├── databricks.yml                      # Databricks Asset Bundle (DAB) root configuration
├── resources/                          # Declarative DAB resource definitions
│   ├── arpa_bg_job.yml                 # Multi-task Databricks Workflow (Bronze -> Silver -> Gold)
│   ├── arpa_bg_pipeline.yml            # Declarative Delta Live Tables (DLT) pipeline (optional)
│   └── arpa_bg_dashboard.yml           # Declarative AI/BI Lakeview dashboard asset
├── config/
│   ├── settings.json                   # Global parameters (dataset IDs, filters, Delta paths)
│   └── pollutants_thresholds.json      # Regulatory thresholds for each pollutant
├── src/
│   ├── __init__.py
│   ├── arpa_client.py                  # HTTP client for paginated SODA API requests
│   └── data_transforms.py              # PySpark functions for cleaning, casting, and joining
├── notebooks/
│   ├── 00_setup_environment.py         # Workspace setup, databases, catalogs, and Delta paths
│   ├── 01_ingest_bronze.py             # Download raw Open Data to Bronze Delta tables
│   ├── 02_transform_silver.py          # BG filtering, stations-measurements join, data cleaning
│   ├── 03_aggregations_gold.py         # Daily/monthly aggregations and exceedance calculations
│   └── 04_dashboard_visualizations.py  # Charts, geographic station maps, and municipal comparisons
└── tests/
    └── test_arpa_client.py             # Unit tests for the extraction client
```

---

## 6. Code Standards & Databricks Notebook Guidelines

All AI agents must adhere to the following standards:

### 6.1 Notebook Format
- Notebooks committed or versioned in git must use standard Databricks Python source format:
  ```python
  # Databricks notebook source
  # MAGIC %md
  # MAGIC # Notebook Title
  # MAGIC Description of the pipeline purpose...

  # COMMAND ----------

  # Python / PySpark code
  ```
- Keep logic cleanly modularized across cells with descriptive Markdown notes (`# MAGIC %md`).

### 6.2 PySpark & Delta Lake Best Practices
- Always use **PySpark DataFrames** and **Spark SQL** for manipulating measurement records (millions of hourly rows).
- Do not invoke `.toPandas()` on raw or unaggregated datasets. Reserve Pandas/Plotly strictly for Gold-level aggregated tables and visualization samples.
- Always persist data using **Delta** format:
  ```python
  df.write.format("delta").mode("append").option("mergeSchema", "true").save(delta_path)
  ```
- For incremental loads, implement Delta `MERGE` operations on composite primary keys (`IdSensore`, `Data`).

### 6.3 Secrets & Configuration Management
- Never hardcode API tokens or credentials in notebook code.
- Always use `dbutils.secrets.get(scope="...", key="...")` or environment variables for Socrata tokens (`X-App-Token`).

### 6.4 Data Quality & Cleansing
- Handle ARPA-specific sentinel values:
  - Readings with non-physical negative values (e.g., `-999`) must be converted to `NULL` or filtered out.
  - Inspect the validation status column `Stato` (e.g., `VA` for validated measurements, `NC` for non-compliant/unvalidated).
- Explicitly cast date/time strings to `TimestampType` and measurements to `DoubleType`.

---

## 7. Visualization & Dashboard Requirements

Analyses covering municipalities across the Province of Bergamo must answer these key questions:

1. **Municipal Comparison & Rankings**:
   - Which municipalities in Bergamo record the highest average concentrations of PM10 and PM2.5?
   - How does pollution differ between urban/plain areas and hilly/mountainous zones?
2. **Regulatory Limit Exceedances**:
   - How many days per year does PM10 exceed the 50 µg/m³ daily threshold for each station/municipality? (Compared against the legal cap of 35 days/year).
3. **Seasonality & Temporal Trends**:
   - Seasonal patterns: winter peaks (domestic heating + thermal inversion) vs. summer peaks (Ozone).
   - Weekday vs. weekend variation.
4. **Geographic Station Map**:
   - Interactive map of the Province of Bergamo displaying station coordinates, sensor types, and current or historical average values.

---

## 8. Databricks Asset Bundles (DAB) — Declarative Assets & CI/CD

The project leverages **Databricks Asset Bundles (DAB)** as the declarative Infrastructure-as-Code (IaC) framework to define, deploy, and manage pipelines, multi-task workflows, schemas, and dashboards across environments.

### 8.1 Root Configuration (`databricks.yml`)

The root `databricks.yml` defines the bundle metadata, environment targets, and custom variables:

```yaml
bundle:
  name: arpa-bg-monitoring

include:
  - resources/*.yml

variables:
  catalog:
    description: "Unity Catalog name for tables"
    default: "hive_metastore"
  schema:
    description: "Target database / schema name"
    default: "arpa_bergamo"
  socrata_secret_scope:
    description: "Databricks secret scope for Socrata App Token"
    default: "arpa_monitoring_scope"
  socrata_secret_key:
    description: "Databricks secret key for Socrata App Token"
    default: "socrata_app_token"

targets:
  dev:
    mode: development
    default: true
    workspace:
      root_path: /Users/${workspace.current_user.userName}/.bundle/${bundle.name}/${bundle.target}
    variables:
      catalog: "dev_catalog"
      schema: "arpa_bg_dev"

  prod:
    mode: production
    workspace:
      root_path: /Shared/.bundle/${bundle.name}/${bundle.target}
    variables:
      catalog: "prod_catalog"
      schema: "arpa_bg_prod"
```

### 8.2 Declarative Workflows (`resources/arpa_bg_job.yml`)

Multi-task workflows orchestrate the Medallion sequence (`Bronze` ➔ `Silver` ➔ `Gold`) declaratively:

- **Task Dependencies**:
  - `ingest_bronze`: Executes [01_ingest_bronze.py](file:///home/michele/projects/arpa-bg-monitoring/notebooks/01_ingest_bronze.py)
  - `transform_silver`: Depends on `ingest_bronze`; executes [02_transform_silver.py](file:///home/michele/projects/arpa-bg-monitoring/notebooks/02_transform_silver.py)
  - `aggregations_gold`: Depends on `transform_silver`; executes [03_aggregations_gold.py](file:///home/michele/projects/arpa-bg-monitoring/notebooks/03_aggregations_gold.py)
- **Parameter Passing**:
  Pass bundle variables into notebook parameters:
  ```yaml
  notebook_task:
    notebook_path: ../notebooks/01_ingest_bronze.py
    base_parameters:
      catalog: ${var.catalog}
      schema: ${var.schema}
      secret_scope: ${var.socrata_secret_scope}
      secret_key: ${var.socrata_secret_key}
  ```
- In notebooks, retrieve parameters using:
  ```python
  catalog = dbutils.widgets.get("catalog")
  schema = dbutils.widgets.get("schema")
  ```

### 8.3 Declarative Dashboards (`resources/arpa_bg_dashboard.yml`)

- Databricks AI/BI (Lakeview) dashboards are versioned as declarative assets in `resources/arpa_bg_dashboard.yml`.
- References the visualization queries based on Gold layer tables (`gold_daily_metrics`, `gold_exceedances`).

### 8.4 CLI Operations & Agent Rules for DAB

All agents modifying workflows or bundle configurations must adhere to the following rules:

1. **Syntax & Path Validation**:
   Always execute `databricks bundle validate` before concluding code changes to ensure all task references, YAML schemas, and notebook relative paths are valid.
2. **Environment Portability**:
   Never hardcode workspace paths, user home folders, or cluster IDs in resource files. Always use bundle substitutions (`${bundle.target}`, `${var.catalog}`, `${workspace.current_user.userName}`).
3. **Deployment Commands**:
   - Validate bundle configuration:
     ```bash
     databricks bundle validate
     ```
   - Deploy bundle to development:
     ```bash
     databricks bundle deploy -t dev
     ```
   - Execute the workflow pipeline in development:
     ```bash
     databricks bundle run -t dev arpa_bg_job
     ```
   - Clean up development deployment:
     ```bash
     databricks bundle destroy -t dev
     ```

---

## 9. Agent Skills & Tooling — Databricks AI Dev Kit

To ensure high accuracy, prevent API hallucinations, and align with Databricks engineering standards, agents collaborating on this project must leverage the **[Databricks AI Dev Kit](https://github.com/databricks-solutions/ai-dev-kit)** (and official Databricks AI Tools).

### 9.1 Overview & Role
The AI Dev Kit equips coding agents with specialized context, modular skills, and execution tools tailored for the Databricks ecosystem:
- **Repository**: [https://github.com/databricks-solutions/ai-dev-kit](https://github.com/databricks-solutions/ai-dev-kit)
- **Role in the Project**: Provides agents with verified rules, prompt guidance, CLI tool wrappers, and schema knowledge for Unity Catalog, Databricks Asset Bundles (DAB), Workflows, and PySpark transformations.

### 9.2 Key Skills Utilized
Agents must consult and apply the following domain-specific skills from the dev kit:

1. **Databricks Asset Bundles (`dab` / `asset-bundles`)**:
   - Guidelines for declarative YAML syntax in [databricks.yml](file:///home/michele/projects/arpa-bg-monitoring/databricks.yml) and [resources/](file:///home/michele/projects/arpa-bg-monitoring/resources/).
   - Variable substitution patterns (`${var.*}`, `${bundle.target}`) and multi-environment targeting (`dev` vs. `prod`).
2. **Unity Catalog & Data Governance (`unity-catalog`)**:
   - Best practices for three-level namespace referencing (`<catalog>.<schema>.<table>`).
   - Management of Delta tables, schema migrations, and Volume storage for Bronze raw files.
3. **Multi-Task Workflows & Jobs (`workflows` / `jobs`)**:
   - Structuring task DAGs, setting retry policies, defining serverless compute, and configuring parameter passing via `base_parameters`.
4. **PySpark & Delta Lake Optimization (`pyspark` / `delta`)**:
   - Optimized patterns for Delta `MERGE` operations, Liquid Clustering, partitioning, schema evolution, and avoiding driver-side anti-patterns (such as unbounded `.collect()` or unneeded `.toPandas()`).
5. **Delta Live Tables & Declarative Pipelines (`dlt-pipelines`)**:
   - Declarative streaming/batch pipelines and data quality expectations (`@dlt.expect_or_drop`).

### 9.3 Installation & Management
- Install and configure skills via the Databricks AI Dev Kit or the official Databricks CLI:
  ```bash
  databricks aitools install
  ```
  or by integrating the skills into the workspace agent customization path (`.agents/skills/`).
- Agents must prioritize the official patterns provided by the dev kit when scaffolding new bundle resources or authoring PySpark notebooks.

---

## 10. Operational Workflow for AI Agents

When implementing or modifying code, agents must follow this sequential process:

1. **Verify Skills & Configuration**:
   - Consult relevant **Databricks AI Dev Kit** skills before generating resources or complex transformations.
   - Check bundle settings in [databricks.yml](file:///home/michele/projects/arpa-bg-monitoring/databricks.yml) and resource files under [resources/](file:///home/michele/projects/arpa-bg-monitoring/resources/).
   - Check parameters in [settings.json](file:///home/michele/projects/arpa-bg-monitoring/config/settings.json) and [pollutants_thresholds.json](file:///home/michele/projects/arpa-bg-monitoring/config/pollutants_thresholds.json).
2. **Develop Incrementally**:
   - Test API extraction with small sample sizes (`$limit=100`).
   - Validate transformation and filtering for Bergamo (`Provincia = 'BG'`).
   - Implement aggregations, KPI tables, and queries for visualizations.
3. **Validate Bundle & Syntax**:
   - Run `databricks bundle validate` to verify YAML definitions and DAG task dependencies.
   - Ensure notebooks are runnable both on Databricks Runtime (DBR) and in local/CI environments.
4. **Deploy & Smoke Test**:
   - Use `databricks bundle deploy -t dev` and trigger test runs via `databricks bundle run -t dev arpa_bg_job`.
5. **Document**:
   - Provide clear docstrings, notebook Markdown annotations, and comments detailing calculated metrics and applied regulatory thresholds.
