# Databricks notebook source
# MAGIC %md
# MAGIC # Gold Layer - Type Casting (Optional)
# MAGIC 
# MAGIC Convert STRING columns to logical data types for analytics optimization.
# MAGIC This is an optional step that creates strongly-typed versions of Gold tables.
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

print(f"Parameters:")
print(f"  Year: {year}")
print(f"  Catalog: {catalog}")
print(f"  Schema: {schema}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Define Type Casting Rules

# COMMAND ----------

# Define column type mappings for each Gold table
type_mappings = {
    "daily_metrics": {
        "trip_date": "date",
        "total_trips": "bigint",
        "total_fare": "decimal(18,2)",
        "avg_fare": "decimal(18,2)",
        "total_tips": "decimal(18,2)",
        "avg_tip": "decimal(18,2)",
        "total_revenue": "decimal(18,2)",
        "avg_revenue": "decimal(18,2)",
        "total_distance": "decimal(18,2)",
        "avg_distance": "decimal(18,2)",
        "avg_passengers": "decimal(10,2)",
        "min_fare": "decimal(18,2)",
        "max_fare": "decimal(18,2)",
        "year": "int"
    },
    "monthly_metrics": {
        "year_month": "string",  # Keep as string (YYYY-MM format)
        "total_trips": "bigint",
        "total_fare": "decimal(18,2)",
        "avg_fare": "decimal(18,2)",
        "total_tips": "decimal(18,2)",
        "avg_tip": "decimal(18,2)",
        "total_revenue": "decimal(18,2)",
        "avg_revenue": "decimal(18,2)",
        "total_distance": "decimal(18,2)",
        "avg_distance": "decimal(18,2)",
        "avg_passengers": "decimal(10,2)",
        "active_days": "int",
        "year": "int"
    },
    "passenger_metrics": {
        "passenger_count": "int",
        "total_trips": "bigint",
        "total_fare": "decimal(18,2)",
        "avg_fare": "decimal(18,2)",
        "total_revenue": "decimal(18,2)",
        "avg_distance": "decimal(18,2)",
        "year": "int"
    }
}

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cast Daily Metrics

# COMMAND ----------

print("=" * 80)
print("CASTING DAILY METRICS TO LOGICAL TYPES")
print("=" * 80)

source_table = f"{catalog}.{schema}.gm_daily_metrics_{year}"
typed_table = f"{catalog}.{schema}.gm_daily_metrics_{year}_typed"

df_daily = spark.table(source_table)
print(f"Source table: {source_table}")
print(f"Target table: {typed_table}")

# Apply type casting
df_typed = df_daily
for col_name, col_type in type_mappings["daily_metrics"].items():
    if col_name in df_typed.columns:
        print(f"  Casting {col_name}: string -> {col_type}")
        df_typed = df_typed.withColumn(col_name, col(col_name).cast(col_type))

# Keep timestamp columns as timestamp
if "aggregation_timestamp" in df_typed.columns:
    df_typed = df_typed.withColumn("aggregation_timestamp", col("aggregation_timestamp").cast("timestamp"))

print(f"\nSchema after type casting:")
df_typed.printSchema()

# Write typed table
(df_typed.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .partitionBy("year")
    .saveAsTable(typed_table))

print(f"✓ Typed table created: {typed_table}")
print()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cast Monthly Metrics

# COMMAND ----------

print("=" * 80)
print("CASTING MONTHLY METRICS TO LOGICAL TYPES")
print("=" * 80)

source_table = f"{catalog}.{schema}.gm_monthly_metrics_{year}"
typed_table = f"{catalog}.{schema}.gm_monthly_metrics_{year}_typed"

df_monthly = spark.table(source_table)
print(f"Source table: {source_table}")
print(f"Target table: {typed_table}")

# Apply type casting
df_typed = df_monthly
for col_name, col_type in type_mappings["monthly_metrics"].items():
    if col_name in df_typed.columns:
        print(f"  Casting {col_name}: string -> {col_type}")
        df_typed = df_typed.withColumn(col_name, col(col_name).cast(col_type))

# Keep timestamp columns as timestamp
if "aggregation_timestamp" in df_typed.columns:
    df_typed = df_typed.withColumn("aggregation_timestamp", col("aggregation_timestamp").cast("timestamp"))

print(f"\nSchema after type casting:")
df_typed.printSchema()

# Write typed table
(df_typed.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .partitionBy("year")
    .saveAsTable(typed_table))

print(f"✓ Typed table created: {typed_table}")
print()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cast Passenger Metrics

# COMMAND ----------

print("=" * 80)
print("CASTING PASSENGER METRICS TO LOGICAL TYPES")
print("=" * 80)

source_table = f"{catalog}.{schema}.gm_passenger_metrics_{year}"
typed_table = f"{catalog}.{schema}.gm_passenger_metrics_{year}_typed"

df_passenger = spark.table(source_table)
print(f"Source table: {source_table}")
print(f"Target table: {typed_table}")

# Apply type casting
df_typed = df_passenger
for col_name, col_type in type_mappings["passenger_metrics"].items():
    if col_name in df_typed.columns:
        print(f"  Casting {col_name}: string -> {col_type}")
        df_typed = df_typed.withColumn(col_name, col(col_name).cast(col_type))

# Keep timestamp columns as timestamp
if "aggregation_timestamp" in df_typed.columns:
    df_typed = df_typed.withColumn("aggregation_timestamp", col("aggregation_timestamp").cast("timestamp"))

print(f"\nSchema after type casting:")
df_typed.printSchema()

# Write typed table
(df_typed.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .partitionBy("year")
    .saveAsTable(typed_table))

print(f"✓ Typed table created: {typed_table}")
print()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validation

# COMMAND ----------

print("=" * 80)
print("TYPE CASTING VALIDATION")
print("=" * 80)

tables_created = [
    f"{catalog}.{schema}.gm_daily_metrics_{year}_typed",
    f"{catalog}.{schema}.gm_monthly_metrics_{year}_typed",
    f"{catalog}.{schema}.gm_passenger_metrics_{year}_typed"
]

for table in tables_created:
    df = spark.table(table)
    row_count = df.count()
    
    print(f"\n📋 {table}")
    print(f"   Rows: {row_count:,}")
    print(f"   Schema:")
    
    for field in df.schema.fields[:5]:  # Show first 5 columns
        print(f"     - {field.name}: {field.dataType.simpleString()}")

print()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

print("=" * 80)
print("TYPE CASTING COMPLETE")
print("=" * 80)
print(f"Year: {year}")
print(f"\nTyped tables created:")
for table in tables_created:
    print(f"  ✓ {table}")
print(f"\nStatus: SUCCESS")
print("=" * 80)
print("\nNOTE: Original STRING-based tables are preserved.")
print("Use '_typed' tables for analytics with strong typing.")
print("=" * 80)

dbutils.notebook.exit(f"SUCCESS: Created {len(tables_created)} typed tables")
