# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Layer Transformation
# MAGIC 
# MAGIC Cleanse and transform Bronze data to Silver layer with data quality improvements.
# MAGIC 
# MAGIC **Parameters:**
# MAGIC - `year`: Year to process
# MAGIC - `catalog`: Unity Catalog name
# MAGIC - `schema`: Schema name

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup and Configuration

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
import json

# Get parameters
dbutils.widgets.text("year", "2023", "Year")
dbutils.widgets.text("catalog", "gm_demo", "Catalog")
dbutils.widgets.text("schema", "gm_test_schema", "Schema")

year = dbutils.widgets.get("year")
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

print(f"Parameters:")
print(f"  Year: {year}")
print(f"  Catalog: {catalog}")
print(f"  Schema: {schema}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Configuration

# COMMAND ----------

# Table names
bronze_table = f"{catalog}.{schema}.gm_data_{year}_bronze"
silver_table = f"{catalog}.{schema}.gm_data_{year}_silver"

# Load data config (hardcoded for now, could be from volume)
config = {
    "key_columns": ["trip_id", "vendor_id"],
    "date_columns": ["pickup_datetime", "dropoff_datetime"],
    "metric_columns": ["fare_amount", "tip_amount", "total_amount", "trip_distance"],
    "dimension_columns": ["passenger_count", "payment_type"]
}

print(f"Source table: {bronze_table}")
print(f"Target table: {silver_table}")
print(f"Key columns: {config['key_columns']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read Bronze Data

# COMMAND ----------

df_bronze = spark.table(bronze_table)
initial_count = df_bronze.count()

print(f"Bronze table records: {initial_count:,}")
print(f"Bronze table columns: {len(df_bronze.columns)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Cleansing

# COMMAND ----------

# Remove duplicates based on key columns
window_spec = Window.partitionBy(*config["key_columns"]).orderBy(col("ingestion_timestamp").desc())

df_deduped = (df_bronze
    .withColumn("row_num", row_number().over(window_spec))
    .filter(col("row_num") == 1)
    .drop("row_num")
)

duplicates_removed = initial_count - df_deduped.count()
print(f"Duplicates removed: {duplicates_removed:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Quality Improvements (Keep as STRING)

# COMMAND ----------

# Clean data - all columns remain as STRING type for Silver layer
# Handle nulls and empty strings
df_clean = df_deduped

for col_name in df_clean.columns:
    # Replace empty strings and "null" strings with actual NULL
    df_clean = df_clean.withColumn(
        col_name,
        when((col(col_name) == "") | (col(col_name) == "null") | (col(col_name) == "NULL"), None)
        .otherwise(col(col_name))
    )

# Add quality flag
df_clean = df_clean.withColumn(
    "data_quality_flag",
    when(
        (col(config["key_columns"][0]).isNull()) |
        (col(config["date_columns"][0]).isNull()),
        "FAILED"
    ).otherwise("PASSED")
)

# Add transformation metadata
df_silver = (df_clean
    .withColumn("silver_processed_timestamp", current_timestamp().cast("string"))
    .withColumn("cleansing_applied", lit("deduplication,null_handling"))
)

print(f"Records after cleansing: {df_silver.count():,}")
print(f"Quality check - PASSED: {df_silver.filter(col('data_quality_flag') == 'PASSED').count():,}")
print(f"Quality check - FAILED: {df_silver.filter(col('data_quality_flag') == 'FAILED').count():,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write to Silver Delta Table

# COMMAND ----------

# Write to Delta table
(df_silver.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .option("delta.enableChangeDataFeed", "true")
    .partitionBy(config["date_columns"][0] if config["date_columns"] else "processing_date")
    .saveAsTable(silver_table))

print(f"✓ Data written to {silver_table}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validate Silver Table

# COMMAND ----------

silver_df = spark.table(silver_table)
final_count = silver_df.count()

print(f"Validation Results:")
print(f"  Input records (Bronze): {initial_count:,}")
print(f"  Output records (Silver): {final_count:,}")
print(f"  Records removed: {initial_count - final_count:,}")
print(f"  Data quality: {silver_df.filter(col('data_quality_flag') == 'PASSED').count() / final_count * 100:.2f}% PASSED")

# Show sample
print(f"\nSample data (5 rows):")
silver_df.limit(5).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

print("=" * 80)
print("SILVER LAYER TRANSFORMATION COMPLETE")
print("=" * 80)
print(f"Year: {year}")
print(f"Table: {silver_table}")
print(f"Records: {final_count:,}")
print(f"Status: SUCCESS")
print("=" * 80)

dbutils.notebook.exit(f"SUCCESS: Transformed {final_count:,} records to {silver_table}")
