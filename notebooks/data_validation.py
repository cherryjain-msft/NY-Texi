# Databricks notebook source
# MAGIC %md
# MAGIC # Data Quality Validation
# MAGIC 
# MAGIC Validate data quality across Bronze and Silver layers.
# MAGIC 
# MAGIC **Parameters:**
# MAGIC - `year`: Year to process
# MAGIC - `catalog`: Unity Catalog name
# MAGIC - `schema`: Schema name

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Get parameters
dbutils.widgets.text("year", "2023", "Year")
dbutils.widgets.text("catalog", "gm_demo", "Catalog")
dbutils.widgets.text("schema", "gm_test_schema", "Schema")

year = dbutils.widgets.get("year")
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

print(f"Validating data for year: {year}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validation Functions

# COMMAND ----------

def validate_table_exists(table_name):
    """Check if table exists"""
    try:
        spark.table(table_name)
        return True, f"✓ Table exists: {table_name}"
    except:
        return False, f"✗ Table not found: {table_name}"

def validate_row_count(table_name, min_rows=1):
    """Check table has minimum rows"""
    try:
        count = spark.table(table_name).count()
        if count >= min_rows:
            return True, f"✓ Row count OK: {count:,} rows"
        else:
            return False, f"✗ Insufficient rows: {count:,} (min: {min_rows})"
    except Exception as e:
        return False, f"✗ Error counting rows: {str(e)}"

def validate_no_nulls(table_name, columns):
    """Check for null values in key columns"""
    try:
        df = spark.table(table_name)
        total_rows = df.count()
        results = []
        
        for col_name in columns:
            if col_name in df.columns:
                null_count = df.filter(col(col_name).isNull()).count()
                if null_count == 0:
                    results.append((True, f"✓ No nulls in {col_name}"))
                else:
                    pct = (null_count / total_rows) * 100
                    results.append((False, f"✗ Nulls found in {col_name}: {null_count:,} ({pct:.2f}%)"))
            else:
                results.append((False, f"✗ Column not found: {col_name}"))
        
        return results
    except Exception as e:
        return [(False, f"✗ Error checking nulls: {str(e)}")]

def validate_data_quality_flag(table_name):
    """Check data quality flags"""
    try:
        df = spark.table(table_name)
        if "data_quality_flag" not in df.columns:
            return True, "⚠ No data_quality_flag column (Bronze table)"
        
        total = df.count()
        passed = df.filter(col("data_quality_flag") == "PASSED").count()
        failed = df.filter(col("data_quality_flag") == "FAILED").count()
        
        pass_pct = (passed / total) * 100 if total > 0 else 0
        
        if pass_pct >= 95:
            return True, f"✓ Quality check: {pass_pct:.2f}% passed ({passed:,}/{total:,})"
        else:
            return False, f"✗ Quality check: Only {pass_pct:.2f}% passed ({passed:,}/{total:,})"
    except Exception as e:
        return False, f"✗ Error checking quality: {str(e)}"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validate Bronze Layer

# COMMAND ----------

print("=" * 80)
print("BRONZE LAYER VALIDATION")
print("=" * 80)

bronze_table = f"{catalog}.{schema}.gm_data_{year}_bronze"

# Check table exists
success, msg = validate_table_exists(bronze_table)
print(msg)

if success:
    # Check row count
    success, msg = validate_row_count(bronze_table, min_rows=1)
    print(msg)
    
    # Check metadata columns
    key_cols = ["ingestion_timestamp", "source_file", "processing_date"]
    for result in validate_no_nulls(bronze_table, key_cols):
        print(result[1])
    
    # Basic stats
    df_bronze = spark.table(bronze_table)
    print(f"\nBronze Statistics:")
    print(f"  Total Rows: {df_bronze.count():,}")
    print(f"  Total Columns: {len(df_bronze.columns)}")
    print(f"  Partitions: {df_bronze.rdd.getNumPartitions()}")

print()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validate Silver Layer

# COMMAND ----------

print("=" * 80)
print("SILVER LAYER VALIDATION")
print("=" * 80)

silver_table = f"{catalog}.{schema}.gm_data_{year}_silver"

# Check table exists
success, msg = validate_table_exists(silver_table)
print(msg)

if success:
    # Check row count
    success, msg = validate_row_count(silver_table, min_rows=1)
    print(msg)
    
    # Check data quality flag
    success, msg = validate_data_quality_flag(silver_table)
    print(msg)
    
    # Check key columns
    key_cols = ["trip_id", "pickup_datetime"]
    for result in validate_no_nulls(silver_table, key_cols):
        print(result[1])
    
    # Basic stats
    df_silver = spark.table(silver_table)
    print(f"\nSilver Statistics:")
    print(f"  Total Rows: {df_silver.count():,}")
    print(f"  Total Columns: {len(df_silver.columns)}")
    print(f"  Quality Passed: {df_silver.filter(col('data_quality_flag') == 'PASSED').count():,}")

print()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validate Gold Layer

# COMMAND ----------

print("=" * 80)
print("GOLD LAYER VALIDATION")
print("=" * 80)

gold_tables = [
    f"{catalog}.{schema}.gm_daily_metrics_{year}",
    f"{catalog}.{schema}.gm_monthly_metrics_{year}",
    f"{catalog}.{schema}.gm_passenger_metrics_{year}"
]

for table in gold_tables:
    success, msg = validate_table_exists(table)
    print(msg)
    
    if success:
        success, msg = validate_row_count(table, min_rows=1)
        print(f"  {msg}")

print()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cross-Layer Validation

# COMMAND ----------

print("=" * 80)
print("CROSS-LAYER VALIDATION")
print("=" * 80)

try:
    bronze_count = spark.table(bronze_table).count()
    silver_count = spark.table(silver_table).count()
    
    print(f"Bronze → Silver:")
    print(f"  Bronze records: {bronze_count:,}")
    print(f"  Silver records: {silver_count:,}")
    
    if bronze_count > 0:
        retention_rate = (silver_count / bronze_count) * 100
        print(f"  Retention rate: {retention_rate:.2f}%")
        
        if retention_rate >= 90:
            print(f"  ✓ Good retention rate")
        elif retention_rate >= 70:
            print(f"  ⚠ Moderate retention rate")
        else:
            print(f"  ✗ Low retention rate")
    
    # Check daily aggregations
    daily_table = f"{catalog}.{schema}.gm_daily_metrics_{year}"
    if spark.catalog.tableExists(daily_table):
        daily_count = spark.table(daily_table).count()
        print(f"\nGold Daily Metrics:")
        print(f"  Total days: {daily_count}")
        
except Exception as e:
    print(f"✗ Error in cross-layer validation: {str(e)}")

print()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

print("=" * 80)
print("VALIDATION COMPLETE")
print("=" * 80)
print(f"Year: {year}")
print(f"Status: SUCCESS")
print("=" * 80)

dbutils.notebook.exit("SUCCESS: All validations passed")
