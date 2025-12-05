# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Layer Ingestion
# MAGIC 
# MAGIC Ingest raw data from volume into Bronze Delta tables with metadata columns.
# MAGIC 
# MAGIC **Parameters:**
# MAGIC - `year`: Year to process (e.g., 2023)
# MAGIC - `catalog`: Unity Catalog name (default: gm_demo)
# MAGIC - `schema`: Schema name (default: gm_test_schema)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup and Configuration

# COMMAND ----------

# Import required libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime
import logging

# Get parameters
dbutils.widgets.text("year", "2023", "Year")
dbutils.widgets.text("catalog", "gm_demo", "Catalog")
dbutils.widgets.text("schema", "gm_test_schema", "Schema")

year = dbutils.widgets.get("year")
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

print(f"Parameters:")
print(f"  Year: {year}")
print(f"  Catalog: {catalog}")
print(f"  Schema: {schema}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Define Configuration

# COMMAND ----------

# Volume and table configuration
# Check if year-specific folder exists, otherwise use base volume path
volume_base = f"/Volumes/{catalog}/{schema}/gm_volume"
volume_with_year = f"{volume_base}/{year}"

# Try year-specific path first, fall back to base path
try:
    dbutils.fs.ls(volume_with_year)
    volume_path = volume_with_year
    print(f"Using year-specific path: {volume_path}")
except:
    volume_path = volume_base
    print(f"Year folder not found, using base path: {volume_path}")

bronze_table = f"{catalog}.{schema}.gm_data_{year}_bronze"

print(f"Source path: {volume_path}")
print(f"Target table: {bronze_table}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read Source Data

# COMMAND ----------

# List files in volume (recursively)
def list_files_recursive(path, max_depth=5, current_depth=0):
    """Recursively list all files in path"""
    all_files = []
    
    try:
        items = dbutils.fs.ls(path)
        
        for item in items:
            if item.isDir() and current_depth < max_depth:
                # Recursively explore subdirectories
                all_files.extend(list_files_recursive(item.path, max_depth, current_depth + 1))
            elif not item.isDir():
                # Add file to list
                all_files.append(item)
        
    except Exception as e:
        print(f"Error listing {path}: {e}")
    
    return all_files

try:
    print(f"Searching for data files in: {volume_path}")
    all_files = list_files_recursive(volume_path)
    
    # Filter for data files (parquet, csv, json)
    data_files = [f for f in all_files if 
                  any(f.name.endswith(ext) for ext in ['.parquet', '.csv', '.json', '.parq'])]
    
    if len(data_files) == 0:
        print(f"No data files found. Listing volume structure:")
        items = dbutils.fs.ls(volume_path)
        for item in items:
            print(f"  {'[DIR]' if item.isDir() else '[FILE]'} {item.name}")
        raise Exception(f"No data files found in {volume_path} or subdirectories")
    
    print(f"Found {len(data_files)} data files")
    for f in data_files[:5]:  # Show first 5
        print(f"  - {f.path}")
        
except Exception as e:
    print(f"Error listing files: {e}")
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ingest Data with Metadata

# COMMAND ----------

# Read data from all found files (auto-detect format)
# Use wildcard pattern to read all parquet files recursively
parquet_pattern = f"{volume_path}/**/*.parquet"
print(f"Reading pattern: {parquet_pattern}")

df = spark.read.format("parquet").load(parquet_pattern)

# Cast all columns to STRING for Bronze layer (raw data preservation)
print(f"Converting all columns to STRING type...")
for column in df.columns:
    df = df.withColumn(column, col(column).cast("string"))

# Add metadata columns
df_bronze = (df
    .withColumn("ingestion_timestamp", current_timestamp().cast("string"))
    .withColumn("source_file", input_file_name())
    .withColumn("processing_date", current_date().cast("string"))
    .withColumn("year", lit(year))
    .withColumn("pipeline_run_id", lit(str(datetime.now().timestamp())))
)

print(f"Schema after adding metadata:")
df_bronze.printSchema()

print(f"\nRow count: {df_bronze.count():,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write to Bronze Delta Table

# COMMAND ----------

# Write to Delta table
(df_bronze.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .option("delta.enableChangeDataFeed", "true")
    .partitionBy("processing_date")
    .saveAsTable(bronze_table))

print(f"✓ Data written to {bronze_table}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validate Ingestion

# COMMAND ----------

# Verify data
bronze_df = spark.table(bronze_table)
row_count = bronze_df.count()
column_count = len(bronze_df.columns)

print(f"Validation Results:")
print(f"  Rows: {row_count:,}")
print(f"  Columns: {column_count}")
print(f"  Table: {bronze_table}")

# Show sample
print(f"\nSample data (5 rows):")
bronze_df.limit(5).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

print("=" * 80)
print("BRONZE LAYER INGESTION COMPLETE")
print("=" * 80)
print(f"Year: {year}")
print(f"Table: {bronze_table}")
print(f"Records: {row_count:,}")
print(f"Status: SUCCESS")
print("=" * 80)

dbutils.notebook.exit(f"SUCCESS: Ingested {row_count:,} records to {bronze_table}")
