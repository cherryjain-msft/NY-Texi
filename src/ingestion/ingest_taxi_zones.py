# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest Taxi Zones Reference Data
# MAGIC 
# MAGIC Loads the taxi zone lookup CSV into Unity Catalog Delta table

# COMMAND ----------

from pyspark.sql.types import *
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# COMMAND ----------

# Get parameters
dbutils.widgets.text("catalog", "nyc_taxi_data", "Catalog Name")
dbutils.widgets.text("source_path", "", "Source Data Path")

catalog = dbutils.widgets.get("catalog")
source_path = dbutils.widgets.get("source_path")

print(f"Catalog: {catalog}")
print(f"Source Path: {source_path}")

# COMMAND ----------

# Define schema for taxi zones
taxi_zones_schema = StructType([
    StructField("LocationID", IntegerType(), True),
    StructField("Borough", StringType(), True),
    StructField("Zone", StringType(), True),
    StructField("service_zone", StringType(), True)
])

# COMMAND ----------

# Read taxi zones CSV
# Handle both volume path and local path formats
if source_path:
    zones_csv_path = f"{source_path}/02.taxi_zones/taxi+_zone_lookup.csv"
else:
    zones_csv_path = "/02.taxi_zones/taxi+_zone_lookup.csv"

logger.info(f"Reading taxi zones from: {zones_csv_path}")

df_zones = spark.read \
    .option("header", "true") \
    .schema(taxi_zones_schema) \
    .csv(zones_csv_path)

row_count = df_zones.count()
logger.info(f"Loaded {row_count} taxi zones")

# COMMAND ----------

# Display sample data
print("\nSample Taxi Zones:")
df_zones.show(10, truncate=False)

# COMMAND ----------

# Write to Delta table (overwrite mode for reference data)
target_table = f"{catalog}.raw.taxi_zones"

logger.info(f"Writing to table: {target_table}")

df_zones.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable(target_table)

print(f"\nSuccessfully loaded {row_count} taxi zones to {target_table}")

# COMMAND ----------

# Verify the table
spark.sql(f"SELECT COUNT(*) as total_zones FROM {target_table}").show()
spark.sql(f"SELECT * FROM {target_table} LIMIT 10").show(truncate=False)

# COMMAND ----------

dbutils.notebook.exit(f"Success: Loaded {row_count} taxi zones")
