# Big Data Analytics Pipeline – Spotify Streaming Dataset

> Scalable end-to-end data engineering pipeline built in Databricks using Apache Spark, PySpark, and Delta Lake, implementing medallion architecture to process and analyze large-scale Spotify streaming data.

---

## Overview

This project demonstrates a production-style data engineering workflow using cloud-native tools. Raw Spotify streaming data is ingested, cleaned, transformed, and aggregated through a multi-layer medallion architecture (Bronze → Silver → Gold), resulting in analytics-ready outputs and visual reports.

**Dataset:** Spotify streaming dataset (~1M+ records)
**Platform:** Databricks (Community or Workspace)
**Core Technologies:** Apache Spark, PySpark, Spark SQL, Delta Lake

---

## Architecture

```
Raw Data (CSV/JSON)
       │
       ▼
  [Bronze Layer]  ← Raw ingestion, no transformation
  Delta Lake Table
       │
       ▼
  [Silver Layer]  ← Cleaned, filtered, schema-optimized
  Delta Lake Table
       │
       ▼
  [Gold Layer]    ← Aggregated, analytics-ready reporting tables
  Delta Lake Table
       │
       ▼
  Visual Reports & Insights
```

---

## Features

- **Medallion Architecture** — Bronze, Silver, Gold layers using Delta Lake tables
- **Large-Scale Preprocessing** — Schema optimization, null handling, filtering, type casting, feature engineering using Spark DataFrames and Spark SQL
- **Delta Lake Management** — ACID transactions, schema enforcement, versioned data tables
- **Analytical Reporting** — Artist performance, music popularity trends, and user listening behavior
- **ELT Pattern** — Raw data loaded first, transformations applied post-ingestion for flexibility
- **Reusable Notebooks** — Modular, documented Databricks notebooks for each pipeline stage

---

## Notebooks

| Notebook | Description |
|----------|-------------|
| `01_bronze_ingestion.ipynb` | Load raw Spotify CSV into Bronze Delta table |
| `02_silver_transformation.ipynb` | Clean, filter, and schema-optimize to Silver layer |
| `03_gold_aggregation.ipynb` | Aggregate to Gold layer — artist stats, popularity trends |
| `04_analytics_reporting.ipynb` | Visual reports and SQL-based analytics queries |

---

## Key Transformations

```python
# Example: Silver layer transformation using PySpark DataFrame API
from pyspark.sql import functions as F

silver_df = bronze_df \
    .dropna(subset=["track_name", "artist_name", "streams"]) \
    .filter(F.col("streams") > 0) \
    .withColumn("streams", F.col("streams").cast("long")) \
    .withColumn("release_year", F.year(F.to_date(F.col("release_date"), "yyyy-MM-dd"))) \
    .dropDuplicates(["track_id"])

silver_df.write.format("delta").mode("overwrite").saveAsTable("silver_spotify")
```

---

## Sample Analytics Queries (Gold Layer)

```sql
-- Top 10 most streamed artists
SELECT artist_name, SUM(streams) AS total_streams
FROM gold_spotify
GROUP BY artist_name
ORDER BY total_streams DESC
LIMIT 10;

-- Yearly popularity trends
SELECT release_year, COUNT(DISTINCT track_id) AS tracks_released, AVG(streams) AS avg_streams
FROM gold_spotify
GROUP BY release_year
ORDER BY release_year;
```

---

## Setup & Run

1. Clone this repository
2. Upload notebooks to your Databricks workspace
3. Attach to a cluster with Spark 3.x and Delta Lake enabled
4. Run notebooks in order: `01 → 02 → 03 → 04`
5. Dataset: [Spotify Streaming Dataset on Kaggle](https://www.kaggle.com/) *(link your actual dataset source)*

---

## Results

- Processed 1M+ streaming records end-to-end across the medallion pipeline
- Identified top artists, trending tracks, and seasonal listening patterns
- Validated data quality at each layer using Delta Lake schema enforcement and SQL assertions

---

## Tech Stack

- **Platform:** Databricks
- **Processing:** Apache Spark, PySpark, Spark SQL
- **Storage:** Delta Lake (Bronze/Silver/Gold tables)
- **Language:** Python, SQL
- **Visualization:** Databricks built-in charts, matplotlib

---

## Author

**Ajayaman Kantumuchu**
MS in Computer Science, CSUSB | ajayamankantumuchu@gmail.com
[LinkedIn](https://linkedin.com/in/ajayaman-k) | [GitHub](https://github.com/Ajayaman2627)
