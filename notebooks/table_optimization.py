# Databricks notebook source
# MAGIC %md
# MAGIC # Table Optimization
# MAGIC 
# MAGIC Optimize Delta tables with OPTIMIZE, Z-ORDER, and optionally VACUUM.
# MAGIC 
# MAGIC **Parameters:**
# MAGIC - `year`: Year to process
# MAGIC - `catalog`: Unity Catalog name
# MAGIC - `schema`: Schema name
# MAGIC - `run_vacuum`: Whether to run VACUUM (true/false)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# Get parameters
dbutils.widgets.text("year", "2023", "Year")
dbutils.widgets.text("catalog", "gm_demo", "Catalog")
dbutils.widgets.text("schema", "gm_test_schema", "Schema")
dbutils.widgets.dropdown("run_vacuum", "false", ["true", "false"], "Run VACUUM")

year = dbutils.widgets.get("year")
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
run_vacuum = dbutils.widgets.get("run_vacuum").lower() == "true"

print(f"Parameters:")
print(f"  Year: {year}")
print(f"  Catalog: {catalog}")
print(f"  Schema: {schema}")
print(f"  Run VACUUM: {run_vacuum}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Define Tables to Optimize

# COMMAND ----------

tables_to_optimize = [
    {
        "name": f"{catalog}.{schema}.gm_data_{year}_bronze",
        "zorder_cols": ["processing_date", "year"]
    },
    {
        "name": f"{catalog}.{schema}.gm_data_{year}_silver",
        "zorder_cols": ["pickup_datetime", "trip_id"]
    },
    {
        "name": f"{catalog}.{schema}.gm_daily_metrics_{year}",
        "zorder_cols": ["trip_date"]
    },
    {
        "name": f"{catalog}.{schema}.gm_monthly_metrics_{year}",
        "zorder_cols": ["year_month"]
    },
    {
        "name": f"{catalog}.{schema}.gm_passenger_metrics_{year}",
        "zorder_cols": ["passenger_count"]
    }
]

print(f"Tables to optimize: {len(tables_to_optimize)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run OPTIMIZE and Z-ORDER

# COMMAND ----------

print("=" * 80)
print("OPTIMIZE AND Z-ORDER")
print("=" * 80)

for table_config in tables_to_optimize:
    table_name = table_config["name"]
    zorder_cols = table_config["zorder_cols"]
    
    # Check if table exists
    try:
        spark.table(table_name)
        print(f"\n📊 Optimizing: {table_name}")
        
        # Run OPTIMIZE with Z-ORDER
        zorder_str = ", ".join(zorder_cols)
        optimize_sql = f"OPTIMIZE {table_name} ZORDER BY ({zorder_str})"
        
        print(f"   Running: OPTIMIZE ZORDER BY ({zorder_str})")
        result = spark.sql(optimize_sql)
        
        # Show results
        result_data = result.collect()[0]
        print(f"   ✓ Files added: {result_data['metrics.numFilesAdded']}")
        print(f"   ✓ Files removed: {result_data['metrics.numFilesRemoved']}")
        
    except Exception as e:
        print(f"   ⚠ Skipped {table_name}: {str(e)}")

print()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run ANALYZE TABLE

# COMMAND ----------

print("=" * 80)
print("ANALYZE TABLE STATISTICS")
print("=" * 80)

for table_config in tables_to_optimize:
    table_name = table_config["name"]
    
    try:
        print(f"\n📊 Analyzing: {table_name}")
        spark.sql(f"ANALYZE TABLE {table_name} COMPUTE STATISTICS")
        print(f"   ✓ Statistics computed")
        
    except Exception as e:
        print(f"   ⚠ Skipped: {str(e)}")

print()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run VACUUM (Optional)

# COMMAND ----------

if run_vacuum:
    print("=" * 80)
    print("VACUUM OLD FILES")
    print("=" * 80)
    
    # Set retention to 7 days (168 hours)
    spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")
    retention_hours = 168
    
    for table_config in tables_to_optimize:
        table_name = table_config["name"]
        
        try:
            print(f"\n🧹 Vacuuming: {table_name}")
            print(f"   Retention: {retention_hours} hours ({retention_hours/24:.0f} days)")
            
            spark.sql(f"VACUUM {table_name} RETAIN {retention_hours} HOURS")
            print(f"   ✓ Old files removed")
            
        except Exception as e:
            print(f"   ⚠ Skipped: {str(e)}")
    
    print()
else:
    print("=" * 80)
    print("VACUUM SKIPPED (run_vacuum = false)")
    print("=" * 80)
    print()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Table Details Summary

# COMMAND ----------

print("=" * 80)
print("TABLE DETAILS")
print("=" * 80)

for table_config in tables_to_optimize:
    table_name = table_config["name"]
    
    try:
        df = spark.table(table_name)
        row_count = df.count()
        col_count = len(df.columns)
        
        # Get table size
        details = spark.sql(f"DESCRIBE DETAIL {table_name}").collect()[0]
        size_bytes = details['sizeInBytes']
        size_mb = size_bytes / (1024 * 1024)
        num_files = details['numFiles']
        
        print(f"\n📋 {table_name}")
        print(f"   Rows: {row_count:,}")
        print(f"   Columns: {col_count}")
        print(f"   Size: {size_mb:.2f} MB")
        print(f"   Files: {num_files}")
        
    except Exception as e:
        print(f"\n⚠ {table_name}: {str(e)}")

print()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

print("=" * 80)
print("OPTIMIZATION COMPLETE")
print("=" * 80)
print(f"Year: {year}")
print(f"Tables optimized: {len(tables_to_optimize)}")
print(f"VACUUM executed: {run_vacuum}")
print(f"Status: SUCCESS")
print("=" * 80)

dbutils.notebook.exit(f"SUCCESS: Optimized {len(tables_to_optimize)} tables")
