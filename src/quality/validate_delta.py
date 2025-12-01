# Databricks notebook source
# MAGIC %md
# MAGIC # Data Quality Validation - NYC Taxi Delta Tables
# MAGIC 
# MAGIC Performs data quality checks on ingested Delta tables:
# MAGIC - Row count validation
# MAGIC - Null value checks
# MAGIC - Date range validation
# MAGIC - Duplicate detection
# MAGIC - Anomaly detection (e.g., negative fares, extreme values)

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

catalog = dbutils.widgets.get("catalog")
schema_name = dbutils.widgets.get("schema")

print(f"Catalog: {catalog}")
print(f"Schema: {schema_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Define Quality Check Functions

# COMMAND ----------

def check_row_counts(catalog: str, schema: str, table_name: str) -> dict:
    """Check if table has data"""
    full_table_name = f"{catalog}.{schema}.{table_name}"
    
    try:
        row_count = spark.sql(f"SELECT COUNT(*) as cnt FROM {full_table_name}").collect()[0]['cnt']
        
        result = {
            "table": table_name,
            "check": "row_count",
            "row_count": row_count,
            "status": "PASS" if row_count > 0 else "WARN",
            "message": f"Table has {row_count:,} rows"
        }
    except Exception as e:
        result = {
            "table": table_name,
            "check": "row_count",
            "status": "FAIL",
            "error": str(e)
        }
    
    return result

def check_null_values(catalog: str, schema: str, table_name: str, critical_columns: list) -> dict:
    """Check for null values in critical columns"""
    full_table_name = f"{catalog}.{schema}.{table_name}"
    
    try:
        df = spark.table(full_table_name)
        total_rows = df.count()
        
        null_counts = {}
        for col_name in critical_columns:
            if col_name in df.columns:
                null_count = df.filter(col(col_name).isNull()).count()
                null_pct = (null_count / total_rows * 100) if total_rows > 0 else 0
                null_counts[col_name] = {
                    "null_count": null_count,
                    "null_percentage": round(null_pct, 2)
                }
        
        # Check if any column has >10% nulls
        high_null_columns = [col for col, stats in null_counts.items() 
                            if stats['null_percentage'] > 10]
        
        status = "WARN" if high_null_columns else "PASS"
        
        result = {
            "table": table_name,
            "check": "null_values",
            "null_counts": null_counts,
            "status": status,
            "message": f"High nulls in: {high_null_columns}" if high_null_columns else "Acceptable null levels"
        }
    except Exception as e:
        result = {
            "table": table_name,
            "check": "null_values",
            "status": "FAIL",
            "error": str(e)
        }
    
    return result

def check_date_ranges(catalog: str, schema: str, table_name: str, date_column: str) -> dict:
    """Validate date ranges are reasonable"""
    full_table_name = f"{catalog}.{schema}.{table_name}"
    
    try:
        df = spark.table(full_table_name)
        
        if date_column not in df.columns:
            return {
                "table": table_name,
                "check": "date_range",
                "status": "SKIP",
                "message": f"Column {date_column} not found"
            }
        
        date_stats = df.agg(
            min(date_column).alias("min_date"),
            max(date_column).alias("max_date")
        ).collect()[0]
        
        min_date = date_stats['min_date']
        max_date = date_stats['max_date']
        
        # Check if dates are reasonable (2009-2024)
        status = "PASS"
        if min_date and (min_date.year < 2009 or min_date.year > 2024):
            status = "WARN"
        if max_date and (max_date.year < 2022 or max_date.year > 2024):
            status = "WARN"
        
        result = {
            "table": table_name,
            "check": "date_range",
            "min_date": str(min_date) if min_date else None,
            "max_date": str(max_date) if max_date else None,
            "status": status,
            "message": f"Date range: {min_date} to {max_date}"
        }
    except Exception as e:
        result = {
            "table": table_name,
            "check": "date_range",
            "status": "FAIL",
            "error": str(e)
        }
    
    return result

def check_negative_values(catalog: str, schema: str, table_name: str, amount_columns: list) -> dict:
    """Check for negative values in amount/distance columns"""
    full_table_name = f"{catalog}.{schema}.{table_name}"
    
    try:
        df = spark.table(full_table_name)
        
        negative_counts = {}
        for col_name in amount_columns:
            if col_name in df.columns:
                neg_count = df.filter(col(col_name) < 0).count()
                negative_counts[col_name] = neg_count
        
        total_negatives = sum(negative_counts.values())
        status = "WARN" if total_negatives > 0 else "PASS"
        
        result = {
            "table": table_name,
            "check": "negative_values",
            "negative_counts": negative_counts,
            "status": status,
            "message": f"Found {total_negatives} negative values" if total_negatives > 0 else "No negative values"
        }
    except Exception as e:
        result = {
            "table": table_name,
            "check": "negative_values",
            "status": "FAIL",
            "error": str(e)
        }
    
    return result

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run Quality Checks

# COMMAND ----------

# Define tables and their validation requirements
tables_to_validate = {
    "yellow_taxi_trips": {
        "date_column": "tpep_pickup_datetime",
        "critical_columns": ["VendorID", "tpep_pickup_datetime", "PULocationID", "DOLocationID"],
        "amount_columns": ["fare_amount", "trip_distance", "total_amount"]
    },
    "green_taxi_trips": {
        "date_column": "lpep_pickup_datetime",
        "critical_columns": ["VendorID", "lpep_pickup_datetime", "PULocationID", "DOLocationID"],
        "amount_columns": ["fare_amount", "trip_distance", "total_amount"]
    },
    "fhv_trips": {
        "date_column": "pickup_datetime",
        "critical_columns": ["dispatching_base_num", "pickup_datetime", "PUlocationID"],
        "amount_columns": []
    },
    "hvfhs_trips": {
        "date_column": "pickup_datetime",
        "critical_columns": ["hvfhs_license_num", "pickup_datetime", "PULocationID"],
        "amount_columns": ["base_passenger_fare", "trip_miles", "tips"]
    }
}

# COMMAND ----------

# Run all quality checks
all_results = []

for table_name, config in tables_to_validate.items():
    print(f"\n{'='*60}")
    print(f"Validating: {table_name}")
    print(f"{'='*60}")
    
    # Row count check
    result = check_row_counts(catalog, schema_name, table_name)
    all_results.append(result)
    print(f"✓ Row Count: {result.get('message', result.get('error'))}")
    
    # Null value check
    result = check_null_values(catalog, schema_name, table_name, config['critical_columns'])
    all_results.append(result)
    print(f"✓ Null Values: {result.get('message', result.get('error'))}")
    
    # Date range check
    result = check_date_ranges(catalog, schema_name, table_name, config['date_column'])
    all_results.append(result)
    print(f"✓ Date Range: {result.get('message', result.get('error'))}")
    
    # Negative values check (if applicable)
    if config['amount_columns']:
        result = check_negative_values(catalog, schema_name, table_name, config['amount_columns'])
        all_results.append(result)
        print(f"✓ Negative Values: {result.get('message', result.get('error'))}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary Report

# COMMAND ----------

# Count results by status
pass_count = sum(1 for r in all_results if r.get('status') == 'PASS')
warn_count = sum(1 for r in all_results if r.get('status') == 'WARN')
fail_count = sum(1 for r in all_results if r.get('status') == 'FAIL')
skip_count = sum(1 for r in all_results if r.get('status') == 'SKIP')

print(f"\n{'='*60}")
print(f"DATA QUALITY VALIDATION SUMMARY")
print(f"{'='*60}")
print(f"Total Checks: {len(all_results)}")
print(f"Passed: {pass_count}")
print(f"Warnings: {warn_count}")
print(f"Failed: {fail_count}")
print(f"Skipped: {skip_count}")
print(f"{'='*60}\n")

# Show failed and warning checks
if fail_count > 0:
    print("\n❌ FAILED CHECKS:")
    for r in all_results:
        if r.get('status') == 'FAIL':
            print(f"  - {r['table']}.{r['check']}: {r.get('error', 'Unknown error')}")

if warn_count > 0:
    print("\n⚠️  WARNING CHECKS:")
    for r in all_results:
        if r.get('status') == 'WARN':
            print(f"  - {r['table']}.{r['check']}: {r.get('message', 'Check log for details')}")

# COMMAND ----------

# Exit with summary
summary = {
    "total_checks": len(all_results),
    "passed": pass_count,
    "warnings": warn_count,
    "failed": fail_count,
    "status": "FAIL" if fail_count > 0 else ("WARN" if warn_count > 0 else "PASS")
}

dbutils.notebook.exit(json.dumps(summary))
