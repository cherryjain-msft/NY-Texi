# Databricks notebook source
# MAGIC %md
# MAGIC # Batch Ingestion - NYC Taxi Parquet to Delta
# MAGIC 
# MAGIC Ingests parquet files from source location into Unity Catalog Delta tables.
# MAGIC Supports all taxi types: yellow, green, FHV, HVFHS

# COMMAND ----------

# Import required libraries
from pyspark.sql import DataFrame
from pyspark.sql.functions import year, month, col, lit, to_timestamp
from pyspark.sql.types import *
from pyspark.sql.utils import AnalysisException
import logging
import json
from datetime import datetime
from pathlib import Path

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Get Parameters

# COMMAND ----------

# Get widget parameters
dbutils.widgets.text("catalog", "nyc_taxi_data", "Catalog Name")
dbutils.widgets.text("taxi_type", "yellow_taxi", "Taxi Type")
dbutils.widgets.text("start_year", "2022", "Start Year")
dbutils.widgets.text("end_year", "2023", "End Year")
dbutils.widgets.text("source_path", "", "Source Data Path")

catalog = dbutils.widgets.get("catalog")
taxi_type = dbutils.widgets.get("taxi_type")
start_year = int(dbutils.widgets.get("start_year"))
end_year = int(dbutils.widgets.get("end_year"))
source_path = dbutils.widgets.get("source_path")

print(f"Configuration:")
print(f"  Catalog: {catalog}")
print(f"  Taxi Type: {taxi_type}")
print(f"  Year Range: {start_year} - {end_year}")
print(f"  Source Path: {source_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Define Helper Functions

# COMMAND ----------

def log_metrics(operation: str, metrics_dict: dict):
    """Log metrics in structured JSON format"""
    log_entry = {
        "timestamp": datetime.utcnow().isoformat(),
        "operation": operation,
        "catalog": catalog,
        "taxi_type": taxi_type,
        **metrics_dict
    }
    print(json.dumps(log_entry))

def get_table_name(taxi_type: str) -> str:
    """Get target table name based on taxi type"""
    table_mapping = {
        "yellow_taxi": "yellow_taxi_trips",
        "green_taxi": "green_taxi_trips",
        "for_hire_vehicle": "fhv_trips",
        "high_volume_for_hire_vehicle": "hvfhs_trips"
    }
    return table_mapping.get(taxi_type, f"{taxi_type}_trips")

def get_datetime_column(taxi_type: str) -> str:
    """Get the primary datetime column for each taxi type"""
    datetime_columns = {
        "yellow_taxi": "tpep_pickup_datetime",
        "green_taxi": "lpep_pickup_datetime",
        "for_hire_vehicle": "pickup_datetime",
        "high_volume_for_hire_vehicle": "pickup_datetime"
    }
    return datetime_columns.get(taxi_type, "pickup_datetime")

def get_source_folder(taxi_type: str) -> str:
    """Get source folder name for taxi type"""
    folder_mapping = {
        "yellow_taxi": "yellow_taxi",
        "green_taxi": "green_taxi",
        "for_hire_vehicle": "for_hire_vehicle",
        "high_volume_for_hire_vehicle": "high_volume_for_hire_vehicle"
    }
    return folder_mapping.get(taxi_type, taxi_type)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configure Spark Settings

# COMMAND ----------

# Enable schema evolution
spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")
spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", "true")
spark.conf.set("spark.databricks.delta.autoCompact.enabled", "true")

# Adaptive query execution
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")

print("Spark configuration set successfully")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ingest Data by Year

# COMMAND ----------

def ingest_year_data(year: int, taxi_type: str, source_base_path: str, target_table: str) -> dict:
    """
    Ingest parquet files for a specific year into Delta table
    
    Returns metrics dictionary with row counts and processing time
    """
    start_time = datetime.now()
    
    try:
        # Construct source path for this year
        source_folder = get_source_folder(taxi_type)
        year_path = f"{source_base_path}/{year}/{source_folder}"
        
        # Check if path exists (for dev/local testing)
        logger.info(f"Reading from: {year_path}")
        
        # Read parquet files for the year
        df = spark.read.parquet(f"{year_path}/*.parquet")
        
        # Get datetime column for this taxi type
        datetime_col = get_datetime_column(taxi_type)
        
        # Ensure datetime column is timestamp type
        if datetime_col in df.columns:
            df = df.withColumn(datetime_col, to_timestamp(col(datetime_col)))
            
            # Add partition columns
            df = df.withColumn("year", year(col(datetime_col))) \
                   .withColumn("month", month(col(datetime_col)))
        else:
            # If datetime column not found, add year/month from folder
            df = df.withColumn("year", lit(year)) \
                   .withColumn("month", lit(1))  # Default to January if no date
        
        row_count = df.count()
        
        # Write to Delta table with schema evolution
        df.write.format("delta") \
          .mode("append") \
          .option("mergeSchema", "true") \
          .partitionBy("year", "month") \
          .saveAsTable(f"{catalog}.raw.{target_table}")
        
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        
        metrics = {
            "year": year,
            "rows_ingested": row_count,
            "duration_seconds": duration,
            "status": "success"
        }
        
        log_metrics("year_ingestion", metrics)
        logger.info(f"Successfully ingested {row_count:,} rows for year {year}")
        
        return metrics
        
    except AnalysisException as e:
        # Path doesn't exist or no data for this year
        logger.warning(f"No data found for year {year}: {str(e)}")
        return {
            "year": year,
            "rows_ingested": 0,
            "status": "skipped",
            "reason": "no_data"
        }
    
    except Exception as e:
        logger.error(f"Error ingesting year {year}: {str(e)}")
        return {
            "year": year,
            "status": "failed",
            "error": str(e)
        }

# COMMAND ----------

# MAGIC %md
# MAGIC ## Main Ingestion Loop

# COMMAND ----------

# Get target table name
target_table = get_table_name(taxi_type)

print(f"\nStarting ingestion for {taxi_type}")
print(f"Target table: {catalog}.raw.{target_table}")
print(f"Year range: {start_year} to {end_year}\n")

# Track overall metrics
total_rows = 0
successful_years = 0
failed_years = 0
all_metrics = []

# Process each year
for year in range(start_year, end_year + 1):
    print(f"\n{'='*60}")
    print(f"Processing year: {year}")
    print(f"{'='*60}")
    
    metrics = ingest_year_data(year, taxi_type, source_path, target_table)
    all_metrics.append(metrics)
    
    if metrics.get("status") == "success":
        successful_years += 1
        total_rows += metrics.get("rows_ingested", 0)
    elif metrics.get("status") == "failed":
        failed_years += 1

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary Statistics

# COMMAND ----------

print(f"\n{'='*60}")
print(f"INGESTION SUMMARY")
print(f"{'='*60}")
print(f"Taxi Type: {taxi_type}")
print(f"Target Table: {catalog}.raw.{target_table}")
print(f"Year Range: {start_year} - {end_year}")
print(f"Successful Years: {successful_years}")
print(f"Failed Years: {failed_years}")
print(f"Total Rows Ingested: {total_rows:,}")
print(f"{'='*60}\n")

# Log final summary
log_metrics("ingestion_complete", {
    "taxi_type": taxi_type,
    "target_table": target_table,
    "year_range": f"{start_year}-{end_year}",
    "successful_years": successful_years,
    "failed_years": failed_years,
    "total_rows": total_rows
})

# Display table info
print(f"\nTable Statistics:")
spark.sql(f"DESCRIBE DETAIL {catalog}.raw.{target_table}").show(truncate=False)

# COMMAND ----------

# Return success if no failures
if failed_years > 0:
    dbutils.notebook.exit(json.dumps({
        "status": "partial_success",
        "successful_years": successful_years,
        "failed_years": failed_years,
        "total_rows": total_rows
    }))
else:
    dbutils.notebook.exit(json.dumps({
        "status": "success",
        "successful_years": successful_years,
        "total_rows": total_rows
    }))
