# Databricks notebook source
# MAGIC %md
# MAGIC # Gold Layer Aggregation
# MAGIC 
# MAGIC Create business-level aggregations from Silver data for analytics and reporting.
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
silver_table = f"{catalog}.{schema}.gm_data_{year}_silver"
gold_daily_table = f"{catalog}.{schema}.gm_daily_metrics_{year}"
gold_monthly_table = f"{catalog}.{schema}.gm_monthly_metrics_{year}"

# Configuration
config = {
    "date_columns": ["pickup_datetime"],
    "metric_columns": ["fare_amount", "tip_amount", "total_amount", "trip_distance"],
    "dimension_columns": ["passenger_count", "payment_type"]
}

print(f"Source table: {silver_table}")
print(f"Target tables:")
print(f"  Daily: {gold_daily_table}")
print(f"  Monthly: {gold_monthly_table}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read Silver Data

# COMMAND ----------

df_silver = spark.table(silver_table)

# Filter only quality-passed records
df_silver = df_silver.filter(col("data_quality_flag") == "PASSED")

initial_count = df_silver.count()
print(f"Silver table records (quality passed): {initial_count:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Daily Aggregations

# COMMAND ----------

# Extract date from timestamp
date_col = config["date_columns"][0]
df_with_date = df_silver.withColumn("trip_date", to_date(col(date_col)))

# Daily aggregations
df_daily = (df_with_date
    .groupBy("trip_date")
    .agg(
        count("*").alias("total_trips"),
        sum("fare_amount").alias("total_fare"),
        avg("fare_amount").alias("avg_fare"),
        sum("tip_amount").alias("total_tips"),
        avg("tip_amount").alias("avg_tip"),
        sum("total_amount").alias("total_revenue"),
        avg("total_amount").alias("avg_revenue"),
        sum("trip_distance").alias("total_distance"),
        avg("trip_distance").alias("avg_distance"),
        avg("passenger_count").alias("avg_passengers"),
        min("fare_amount").alias("min_fare"),
        max("fare_amount").alias("max_fare")
    )
    .withColumn("aggregation_timestamp", current_timestamp())
    .withColumn("year", lit(year))
)

print(f"Daily aggregations created: {df_daily.count():,} days")

# Show sample
print("\nSample daily metrics:")
df_daily.orderBy(col("trip_date").desc()).limit(5).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write Daily Aggregations

# COMMAND ----------

(df_daily.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .partitionBy("year")
    .saveAsTable(gold_daily_table))

print(f"✓ Daily metrics written to {gold_daily_table}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Monthly Aggregations

# COMMAND ----------

# Monthly aggregations
df_monthly = (df_with_date
    .withColumn("year_month", date_format(col("trip_date"), "yyyy-MM"))
    .groupBy("year_month")
    .agg(
        count("*").alias("total_trips"),
        sum("fare_amount").alias("total_fare"),
        avg("fare_amount").alias("avg_fare"),
        sum("tip_amount").alias("total_tips"),
        avg("tip_amount").alias("avg_tip"),
        sum("total_amount").alias("total_revenue"),
        avg("total_amount").alias("avg_revenue"),
        sum("trip_distance").alias("total_distance"),
        avg("trip_distance").alias("avg_distance"),
        avg("passenger_count").alias("avg_passengers"),
        countDistinct("trip_date").alias("active_days")
    )
    .withColumn("aggregation_timestamp", current_timestamp())
    .withColumn("year", lit(year))
)

print(f"Monthly aggregations created: {df_monthly.count():,} months")

# Show sample
print("\nSample monthly metrics:")
df_monthly.orderBy(col("year_month").desc()).limit(5).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write Monthly Aggregations

# COMMAND ----------

(df_monthly.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .partitionBy("year")
    .saveAsTable(gold_monthly_table))

print(f"✓ Monthly metrics written to {gold_monthly_table}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Dimension-based Aggregations

# COMMAND ----------

# Aggregation by passenger count
gold_passenger_table = f"{catalog}.{schema}.gm_passenger_metrics_{year}"

df_passenger = (df_silver
    .groupBy("passenger_count")
    .agg(
        count("*").alias("total_trips"),
        sum("fare_amount").alias("total_fare"),
        avg("fare_amount").alias("avg_fare"),
        sum("total_amount").alias("total_revenue"),
        avg("trip_distance").alias("avg_distance")
    )
    .withColumn("aggregation_timestamp", current_timestamp())
    .withColumn("year", lit(year))
)

(df_passenger.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(gold_passenger_table))

print(f"✓ Passenger metrics written to {gold_passenger_table}")
df_passenger.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

daily_count = spark.table(gold_daily_table).count()
monthly_count = spark.table(gold_monthly_table).count()
passenger_count = spark.table(gold_passenger_table).count()

print("=" * 80)
print("GOLD LAYER AGGREGATION COMPLETE")
print("=" * 80)
print(f"Year: {year}")
print(f"Source records: {initial_count:,}")
print(f"\nCreated tables:")
print(f"  Daily metrics: {gold_daily_table} ({daily_count} records)")
print(f"  Monthly metrics: {gold_monthly_table} ({monthly_count} records)")
print(f"  Passenger metrics: {gold_passenger_table} ({passenger_count} records)")
print(f"\nStatus: SUCCESS")
print("=" * 80)

dbutils.notebook.exit(f"SUCCESS: Created {daily_count + monthly_count + passenger_count} aggregation records")
