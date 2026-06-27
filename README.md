# Big Data Analytics Pipeline – Amazon Ecommerce Dataset

> Scalable end-to-end data engineering pipeline built in Databricks using Apache Spark, PySpark, and Delta Lake, implementing medallion architecture to process and analyze large-scale Amazon ecommerce sales data.

---

## Overview

This project demonstrates a production-style data engineering workflow using cloud-native tools. Raw Amazon ecommerce sales data is ingested, cleaned, transformed, and aggregated through a multi-layer medallion architecture (Bronze → Silver → Gold), resulting in analytics-ready outputs and visual reports.

**Dataset:** Amazon ecommerce sales dataset (~1M+ records)
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
  Delta Lake Table (silver_amazon_sales)
       │
       ▼
  [Gold Layer]    ← Aggregated, analytics-ready reporting tables
  Delta Lake Tables (gold_category_sales, gold_monthly_sales, etc.)
       │
       ▼
  Visual Reports & Insights
```

---

## Features

- **Medallion Architecture** — Bronze, Silver, Gold layers using Delta Lake tables
- **Large-Scale Preprocessing** — Schema optimization, null handling, filtering, type casting, feature engineering using Spark DataFrames and Spark SQL
- **Delta Lake Management** — ACID transactions, schema enforcement, versioned data tables
- **Analytical Reporting** — Category performance, product rankings, payment method analysis, monthly revenue trends
- **ELT Pattern** — Raw data loaded first, transformations applied post-ingestion for flexibility
- **Reusable Notebooks** — Modular, documented Databricks notebooks for each pipeline stage

---

## Gold Layer Tables

| Table | Description |
|-------|-------------|
| `gold_category_sales` | Revenue and order counts aggregated by product category |
| `gold_monthly_sales` | Monthly revenue and order trends over time |
| `gold_payment_sales` | Revenue breakdown by payment method |
| `gold_top_products` | Top-selling products ranked by total revenue |

---

## Key Transformations

### Product Ranking with Window Functions

```python
from pyspark.sql.window import Window
from pyspark.sql.functions import dense_rank, round, sum, col

silver_df = spark.table("silver_amazon_sales")

product_rank_df = silver_df.groupBy("Category", "ProductName").agg(
    round(sum("TotalAmount"), 2).alias("ProductRevenue")
)

window_spec = Window.partitionBy("Category").orderBy(col("ProductRevenue").desc())

top_products_per_category = product_rank_df.withColumn(
    "RankInCategory",
    dense_rank().over(window_spec)
).filter(col("RankInCategory") <= 3)
```

---

## Sample Analytics Queries (Gold Layer)

```sql
-- Top 10 products by revenue
SELECT ProductName, Category, Brand,
       SUM(Quantity) AS total_units_sold,
       ROUND(SUM(TotalAmount), 2) AS total_revenue
FROM silver_amazon_sales
GROUP BY ProductName, Category, Brand
ORDER BY total_revenue DESC
LIMIT 10;

-- Revenue by payment method
SELECT PaymentMethod,
       ROUND(SUM(TotalAmount), 2) AS total_revenue,
       COUNT(DISTINCT OrderID) AS total_orders
FROM silver_amazon_sales
GROUP BY PaymentMethod
ORDER BY total_revenue DESC;

-- Monthly revenue trends
SELECT YearMonth,
       ROUND(SUM(TotalAmount), 2) AS monthly_revenue,
       COUNT(DISTINCT OrderID) AS monthly_orders
FROM silver_amazon_sales
GROUP BY YearMonth
ORDER BY YearMonth;
```

---

## Setup & Run

1. Clone this repository
2. Upload notebooks to your Databricks workspace
3. Attach to a cluster with Spark 3.x and Delta Lake enabled
4. Upload your Amazon ecommerce CSV dataset to DBFS
5. Run the notebook to execute the full pipeline

---

## Results

- Processed 1M+ ecommerce transaction records end-to-end across the medallion pipeline
- Identified top products, category performance, and seasonal revenue trends
- Analyzed payment method distribution and monthly order patterns
- Validated data quality at each layer using Delta Lake schema enforcement and SQL assertions

---

## Project Structure

```
spotify-bigdata-pipeline/
├── notebook/
│   ├── Ecommerce_Pipeline.py       # Databricks notebook (Python)
│   └── Ecommerce_Pipeline.ipynb    # Jupyter notebook format
└── README.md
```

---

## Tech Stack

- **Platform:** Databricks
- **Processing:** Apache Spark, PySpark, Spark SQL
- **Storage:** Delta Lake (Bronze/Silver/Gold tables)
- **Language:** Python, SQL
- **Visualization:** Databricks built-in charts

---

## Author

**Ajayaman Kantumuchu**
MS in Computer Science, CSUSB | ajayamankantumuchu@gmail.com
[LinkedIn](https://linkedin.com/in/ajayaman-k) | [GitHub](https://github.com/Ajayaman2627)
