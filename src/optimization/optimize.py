# Databricks notebook source
# MAGIC %md
# MAGIC # Delta Table Optimization
# MAGIC 
# MAGIC Performs optimization operations on Delta tables:
# MAGIC - OPTIMIZE with Z-ORDERING
# MAGIC - VACUUM old files
# MAGIC - ANALYZE table statistics

# COMMAND ----------

from pyspark.sql.functions import *
from datetime import datetime
import json
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# COMMAND ----------

# Get parameters
dbutils.widgets.text("catalog", "nyc_taxi_data", "Catalog Name")
dbutils.widgets.text("schema", "raw", "Schema Name")
dbutils.widgets.dropdown("operation", "optimize", ["optimize", "vacuum", "analyze", "all"], "Operation")

catalog = dbutils.widgets.get("catalog")
schema_name = dbutils.widgets.get("schema")
operation = dbutils.widgets.get("operation")

print(f"Catalog: {catalog}")
print(f"Schema: {schema_name}")
print(f"Operation: {operation}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Define Optimization Functions

# COMMAND ----------

def optimize_table(catalog: str, schema: str, table_name: str, zorder_columns: list = None) -> dict:
    """
    Run OPTIMIZE on a Delta table with optional Z-ORDERING
    """
    start_time = datetime.now()
    full_table_name = f"{catalog}.{schema}.{table_name}"
    
    try:
        logger.info(f"Optimizing table: {full_table_name}")
        
        # Build OPTIMIZE command
        if zorder_columns:
            zorder_cols = ", ".join(zorder_columns)
            optimize_cmd = f"OPTIMIZE {full_table_name} ZORDER BY ({zorder_cols})"
        else:
            optimize_cmd = f"OPTIMIZE {full_table_name}"
        
        result = spark.sql(optimize_cmd)
        
        # Get metrics from result
        metrics = result.select("metrics.*").collect()[0].asDict()
        
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        
        return {
            "table": table_name,
            "operation": "optimize",
            "status": "success",
            "duration_seconds": duration,
            "num_files_added": metrics.get('numFilesAdded', 0),
            "num_files_removed": metrics.get('numFilesRemoved', 0),
            "zorder_columns": zorder_columns
        }
    
    except Exception as e:
        logger.error(f"Error optimizing {full_table_name}: {str(e)}")
        return {
            "table": table_name,
            "operation": "optimize",
            "status": "failed",
            "error": str(e)
        }

def vacuum_table(catalog: str, schema: str, table_name: str, retention_hours: int = 168) -> dict:
    """
    Run VACUUM on a Delta table to remove old files
    Default retention: 168 hours (7 days)
    """
    start_time = datetime.now()
    full_table_name = f"{catalog}.{schema}.{table_name}"
    
    try:
        logger.info(f"Vacuuming table: {full_table_name} (retention: {retention_hours} hours)")
        
        # Run VACUUM
        vacuum_cmd = f"VACUUM {full_table_name} RETAIN {retention_hours} HOURS"
        spark.sql(vacuum_cmd)
        
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        
        return {
            "table": table_name,
            "operation": "vacuum",
            "status": "success",
            "duration_seconds": duration,
            "retention_hours": retention_hours
        }
    
    except Exception as e:
        logger.error(f"Error vacuuming {full_table_name}: {str(e)}")
        return {
            "table": table_name,
            "operation": "vacuum",
            "status": "failed",
            "error": str(e)
        }

def analyze_table(catalog: str, schema: str, table_name: str) -> dict:
    """
    Run ANALYZE TABLE to compute statistics
    """
    start_time = datetime.now()
    full_table_name = f"{catalog}.{schema}.{table_name}"
    
    try:
        logger.info(f"Analyzing table: {full_table_name}")
        
        # Compute statistics
        analyze_cmd = f"ANALYZE TABLE {full_table_name} COMPUTE STATISTICS FOR ALL COLUMNS"
        spark.sql(analyze_cmd)
        
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        
        return {
            "table": table_name,
            "operation": "analyze",
            "status": "success",
            "duration_seconds": duration
        }
    
    except Exception as e:
        logger.error(f"Error analyzing {full_table_name}: {str(e)}")
        return {
            "table": table_name,
            "operation": "analyze",
            "status": "failed",
            "error": str(e)
        }

# COMMAND ----------

# MAGIC %md
# MAGIC ## Define Tables and Z-ORDER Columns

# COMMAND ----------

# Define tables with their optimal Z-ORDER columns
table_configs = {
    "yellow_taxi_trips": {
        "zorder_columns": ["PULocationID", "tpep_pickup_datetime"]
    },
    "green_taxi_trips": {
        "zorder_columns": ["PULocationID", "lpep_pickup_datetime"]
    },
    "fhv_trips": {
        "zorder_columns": ["PUlocationID", "pickup_datetime"]
    },
    "hvfhs_trips": {
        "zorder_columns": ["PULocationID", "pickup_datetime"]
    },
    "taxi_zones": {
        "zorder_columns": ["LocationID"]
    }
}

# COMMAND ----------

# MAGIC %md
# MAGIC ## Execute Operations

# COMMAND ----------

all_results = []

for table_name, config in table_configs.items():
    print(f"\n{'='*60}")
    print(f"Processing table: {table_name}")
    print(f"{'='*60}")
    
    try:
        # Check if table exists
        table_exists = spark.catalog.tableExists(f"{catalog}.{schema_name}.{table_name}")
        
        if not table_exists:
            print(f"⚠️  Table {table_name} does not exist, skipping...")
            continue
        
        # OPTIMIZE operation
        if operation in ["optimize", "all"]:
            print(f"\n📊 Running OPTIMIZE...")
            result = optimize_table(
                catalog, 
                schema_name, 
                table_name, 
                config.get('zorder_columns')
            )
            all_results.append(result)
            
            if result['status'] == 'success':
                print(f"✓ Optimized: Added {result['num_files_added']}, Removed {result['num_files_removed']} files")
                print(f"  Z-ORDER columns: {result.get('zorder_columns', 'None')}")
                print(f"  Duration: {result['duration_seconds']:.2f} seconds")
            else:
                print(f"✗ Failed: {result.get('error')}")
        
        # VACUUM operation
        if operation in ["vacuum", "all"]:
            print(f"\n🧹 Running VACUUM...")
            result = vacuum_table(catalog, schema_name, table_name, retention_hours=168)
            all_results.append(result)
            
            if result['status'] == 'success':
                print(f"✓ Vacuumed: Retention {result['retention_hours']} hours")
                print(f"  Duration: {result['duration_seconds']:.2f} seconds")
            else:
                print(f"✗ Failed: {result.get('error')}")
        
        # ANALYZE operation
        if operation in ["analyze", "all"]:
            print(f"\n📈 Running ANALYZE...")
            result = analyze_table(catalog, schema_name, table_name)
            all_results.append(result)
            
            if result['status'] == 'success':
                print(f"✓ Analyzed statistics")
                print(f"  Duration: {result['duration_seconds']:.2f} seconds")
            else:
                print(f"✗ Failed: {result.get('error')}")
        
        # Show table details
        print(f"\n📋 Table Details:")
        spark.sql(f"DESCRIBE DETAIL {catalog}.{schema_name}.{table_name}") \
            .select("numFiles", "sizeInBytes", "properties") \
            .show(truncate=False)
    
    except Exception as e:
        logger.error(f"Error processing table {table_name}: {str(e)}")
        all_results.append({
            "table": table_name,
            "operation": operation,
            "status": "failed",
            "error": str(e)
        })

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary Report

# COMMAND ----------

success_count = sum(1 for r in all_results if r.get('status') == 'success')
failed_count = sum(1 for r in all_results if r.get('status') == 'failed')

print(f"\n{'='*60}")
print(f"OPTIMIZATION SUMMARY")
print(f"{'='*60}")
print(f"Operation: {operation}")
print(f"Total Operations: {len(all_results)}")
print(f"Successful: {success_count}")
print(f"Failed: {failed_count}")
print(f"{'='*60}\n")

if failed_count > 0:
    print("❌ FAILED OPERATIONS:")
    for r in all_results:
        if r.get('status') == 'failed':
            print(f"  - {r['table']}.{r['operation']}: {r.get('error')}")

# Log results
for result in all_results:
    print(json.dumps(result))

# COMMAND ----------

# Exit with summary
summary = {
    "operation": operation,
    "total_operations": len(all_results),
    "successful": success_count,
    "failed": failed_count,
    "status": "success" if failed_count == 0 else "partial_failure"
}

dbutils.notebook.exit(json.dumps(summary))
