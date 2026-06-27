# Databricks notebook source
gold_category_sales = spark.table("gold_category_sales")
gold_monthly_sales = spark.table("gold_monthly_sales")
gold_payment_sales = spark.table("gold_payment_sales")
gold_top_products = spark.table("gold_top_products")

display(gold_category_sales)
display(gold_monthly_sales)
display(gold_payment_sales)
display(gold_top_products.limit(10))

# COMMAND ----------

gold_category_sales = spark.table("gold_category_sales")
display(gold_category_sales)

# COMMAND ----------

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

top_products_per_category.show()

# COMMAND ----------

# DBTITLE 1,Cell 1
spark.sql("""
SELECT ProductName,
       Category,
       Brand,
       SUM(Quantity) AS total_units_sold,
       ROUND(SUM(TotalAmount), 2) AS total_revenue
FROM silver_amazon_sales
GROUP BY ProductName, Category, Brand
ORDER BY total_revenue DESC
LIMIT 10
""").show()

# COMMAND ----------

# DBTITLE 1,Cell 1
spark.sql("""
SELECT PaymentMethod,
       ROUND(SUM(TotalAmount), 2) AS total_revenue,
       COUNT(DISTINCT OrderID) AS total_orders
FROM silver_amazon_sales
GROUP BY PaymentMethod
ORDER BY total_revenue DESC
""").show()

# COMMAND ----------

# DBTITLE 1,Cell 1
spark.sql("""
SELECT YearMonth,
       ROUND(SUM(TotalAmount), 2) AS monthly_revenue,
       COUNT(DISTINCT OrderID) AS monthly_orders
FROM silver_amazon_sales
GROUP BY YearMonth
ORDER BY YearMonth
""").show()

# COMMAND ----------

# DBTITLE 1,Cell 1
spark.sql("""
SELECT Category,
       ROUND(SUM(TotalAmount), 2) AS total_revenue,
       SUM(Quantity) AS total_units,
       COUNT(DISTINCT OrderID) AS total_orders
FROM silver_amazon_sales
GROUP BY Category
ORDER BY total_revenue DESC
""").show()

# COMMAND ----------

from pyspark.sql.functions import sum, avg, countDistinct, round, col

# COMMAND ----------

gold_top_products = silver_df.groupBy("ProductName", "Category", "Brand").agg(
    sum("Quantity").alias("TotalUnitsSold"),
    round(sum("TotalAmount"), 2).alias("TotalRevenue")
).orderBy(col("TotalRevenue").desc())

gold_top_products.write.mode("overwrite").saveAsTable("gold_top_products")
gold_top_products.show(10)

# COMMAND ----------

gold_geo_sales = silver_df.groupBy("Country", "State").agg(
    round(sum("TotalAmount"), 2).alias("TotalRevenue"),
    countDistinct("OrderID").alias("TotalOrders")
).orderBy(col("TotalRevenue").desc())

gold_geo_sales.write.mode("overwrite").saveAsTable("gold_geo_sales")
gold_geo_sales.show()

# COMMAND ----------

gold_monthly_sales = silver_df.groupBy("YearMonth").agg(
    round(sum("TotalAmount"), 2).alias("MonthlyRevenue"),
    countDistinct("OrderID").alias("MonthlyOrders")
).orderBy("YearMonth")

gold_monthly_sales.write.mode("overwrite").saveAsTable("gold_monthly_sales")
gold_monthly_sales.show()

# COMMAND ----------

gold_payment_sales = silver_df.groupBy("PaymentMethod").agg(
    round(sum("TotalAmount"), 2).alias("TotalRevenue"),
    countDistinct("OrderID").alias("TotalOrders")
).orderBy(col("TotalRevenue").desc())

gold_payment_sales.write.mode("overwrite").saveAsTable("gold_payment_sales")
gold_payment_sales.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold Layer
# MAGIC
# MAGIC The Gold layer contains business-ready aggregated tables built from the Silver layer.  
# MAGIC These tables support reporting and analytical queries such as revenue by category, revenue by payment method, geographic sales distribution, and monthly trends.

# COMMAND ----------

silver_df.select("TotalAmount", "FinalCalculatedAmount", "AmountDifference").show(10)

# COMMAND ----------

silver_df.count()

# COMMAND ----------

# DBTITLE 1,Cell 1
from pyspark.sql.functions import sum, when, col

display(
    silver_df.select([
        sum(when(col(c).isNull(), 1).otherwise(0)).alias(c)
        for c in silver_df.columns
    ])
)

# COMMAND ----------

silver_df = spark.table("silver_amazon_sales")
silver_df.show(10)
silver_df.printSchema()

# COMMAND ----------

silver_df.select("TotalAmount", "FinalCalculatedAmount", "AmountDifference").show(10)

# COMMAND ----------

silver_df = spark.table("silver_amazon_sales")
silver_df.show(10)
silver_df.printSchema()

# COMMAND ----------

silver_df.write.mode("overwrite").saveAsTable("silver_amazon_sales")

# COMMAND ----------

from pyspark.sql.functions import abs

silver_df = silver_df.withColumn(
    "AmountDifference",
    round(abs(col("TotalAmount") - col("FinalCalculatedAmount")), 2)
)

# COMMAND ----------

from pyspark.sql.functions import month, year, date_format, round

silver_df = silver_df.withColumn("GrossAmount", round(col("Quantity") * col("UnitPrice"), 2)) \
                     .withColumn("NetBeforeTax", round(col("GrossAmount") - col("Discount"), 2)) \
                     .withColumn("FinalCalculatedAmount", round(col("NetBeforeTax") + col("Tax") + col("ShippingCost"), 2)) \
                     .withColumn("OrderYear", year(col("OrderDate"))) \
                     .withColumn("OrderMonth", month(col("OrderDate"))) \
                     .withColumn("YearMonth", date_format(col("OrderDate"), "yyyy-MM"))

# COMMAND ----------

from pyspark.sql.functions import initcap, trim

text_cols = ["CustomerName", "ProductName", "Category", "Brand",
             "PaymentMethod", "OrderStatus", "City", "State", "Country"]

for c in text_cols:
    silver_df = silver_df.withColumn(c, initcap(trim(col(c))))

# COMMAND ----------

silver_df = silver_df.filter(col("OrderID").isNotNull()) \
                     .filter(col("CustomerID").isNotNull()) \
                     .filter(col("ProductID").isNotNull()) \
                     .filter(col("SellerID").isNotNull()) \
                     .filter(col("OrderDate").isNotNull()) \
                     .filter(col("Quantity") > 0) \
                     .filter(col("UnitPrice") >= 0) \
                     .filter(col("Discount") >= 0) \
                     .filter(col("Tax") >= 0) \
                     .filter(col("ShippingCost") >= 0) \
                     .filter(col("TotalAmount") >= 0)

# COMMAND ----------

silver_df = silver_df.fillna({
    "CustomerName": "Unknown",
    "ProductName": "Unknown",
    "Category": "Unknown",
    "Brand": "Unknown",
    "PaymentMethod": "Unknown",
    "OrderStatus": "Unknown",
    "City": "Unknown",
    "State": "Unknown",
    "Country": "Unknown"
})

# COMMAND ----------

from pyspark.sql.functions import col, to_date
from pyspark.sql.types import IntegerType, DoubleType

silver_df = silver_df.withColumn("OrderDate", to_date(col("OrderDate"), "yyyy-MM-dd"))

silver_df = silver_df.withColumn("Quantity", col("Quantity").cast(IntegerType())) \
                     .withColumn("UnitPrice", col("UnitPrice").cast(DoubleType())) \
                     .withColumn("Discount", col("Discount").cast(DoubleType())) \
                     .withColumn("Tax", col("Tax").cast(DoubleType())) \
                     .withColumn("ShippingCost", col("ShippingCost").cast(DoubleType())) \
                     .withColumn("TotalAmount", col("TotalAmount").cast(DoubleType()))

# COMMAND ----------

silver_df = silver_df.dropDuplicates()


# COMMAND ----------

silver_df = spark.table("bronze_amazon_sales")

# COMMAND ----------

## Silver Layer

The Silver layer contains cleaned and standardized data derived from the Bronze layer.  
In this stage, duplicate rows, null values, invalid numeric values, and inconsistent data types are handled using Spark DataFrames.  
Additional derived metrics are also created to support downstream analytics.


# COMMAND ----------

print(silver_df.columns)

# COMMAND ----------

silver_df.printSchema()

# COMMAND ----------

silver_df = silver_df.filter(col("OrderID").isNotNull())
silver_df = silver_df.filter(col("ProductID").isNotNull())
silver_df = silver_df.filter(col("CustomerID").isNotNull())

# COMMAND ----------

silver_df = silver_df.fillna({
    "CustomerName": "Unknown",
    "ProductName": "Unknown",
    "Category": "Unknown"
})

# COMMAND ----------

from pyspark.sql.functions import to_date

silver_df = silver_df.withColumn("OrderDate", to_date(col("OrderDate"), "yyyy-MM-dd"))

# COMMAND ----------

silver_df = bronze_df.dropDuplicates()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver Layer
# MAGIC
# MAGIC The Silver layer contains cleaned and transformed data derived from the Bronze layer.  
# MAGIC In this stage, duplicates, null values, invalid records, and inconsistent data types are handled using Spark DataFrames.

# COMMAND ----------

bronze_df.groupBy("OrderID").count().filter("count > 1").show()

# COMMAND ----------

from pyspark.sql.functions import col, sum, when

bronze_df.select([
    sum(when(col(c).isNull(), 1).otherwise(0)).alias(c)
    for c in bronze_df.columns
]).show()

# COMMAND ----------

bronze_df.columns

# COMMAND ----------

bronze_df.count()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze Layer
# MAGIC
# MAGIC The Bronze layer stores the raw ingested e-commerce dataset with minimal modification.  
# MAGIC This layer preserves the source data for traceability, reproducibility, and future reprocessing.  
# MAGIC In this project, the uploaded `amazon` table is copied into a Bronze table named `bronze_amazon_sales`.

# COMMAND ----------

bronze_df = spark.table("bronze_amazon_sales")
bronze_df.show(5)

# COMMAND ----------

bronze_df = amazon_df

bronze_df.write.mode("overwrite").saveAsTable("bronze_amazon_sales")

# COMMAND ----------

display(amazon_df)

# COMMAND ----------

amazon_df.columns

# COMMAND ----------

amazon_df.printSchema()
amazon_df.show(5)

# COMMAND ----------

amazon_df = spark.table("amazon")