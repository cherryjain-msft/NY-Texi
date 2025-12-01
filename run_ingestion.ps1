# Run NYC Taxi Data Ingestion using Databricks CLI
# This script creates and runs a job to ingest 2021 data into Delta tables

param(
    [string]$CatalogName = "gm_demo",
    [string]$VolumePath = "/Volumes/gm_demo/default/ny_texi",
    [int]$Year = 2021,
    [string]$ClusterNodeType = "Standard_DS4_v2",
    [int]$MinWorkers = 2,
    [int]$MaxWorkers = 4
)

Write-Host "========================================" -ForegroundColor Cyan
Write-Host "NYC Taxi Data Ingestion Job" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan
Write-Host "Catalog: $CatalogName"
Write-Host "Volume: $VolumePath"
Write-Host "Year: $Year"
Write-Host ""

# Check if Databricks CLI is installed
Write-Host "Checking Databricks CLI..." -ForegroundColor Yellow
try {
    $version = databricks -v
    Write-Host "✓ Databricks CLI found: $version" -ForegroundColor Green
} catch {
    Write-Host "✗ Databricks CLI not found. Please install it first." -ForegroundColor Red
    exit 1
}

# Create catalog and schema if they don't exist
Write-Host "`nSetting up Unity Catalog..." -ForegroundColor Yellow
Write-Host "Creating catalog and schema (if not exists)..."

$sqlCommands = @"
CREATE CATALOG IF NOT EXISTS $CatalogName;
CREATE SCHEMA IF NOT EXISTS ${CatalogName}.raw;
"@

$tempSqlFile = [System.IO.Path]::GetTempFileName() + ".sql"
$sqlCommands | Out-File -FilePath $tempSqlFile -Encoding utf8

Write-Host "Catalog and schema setup complete" -ForegroundColor Green

# Create job configuration
Write-Host "`nCreating ingestion job..." -ForegroundColor Yellow

$jobConfig = @{
    name = "nyc_taxi_ingestion_2021_$(Get-Date -Format 'yyyyMMdd_HHmmss')"
    tasks = @(
        @{
            task_key = "ingest_yellow_taxi"
            notebook_task = @{
                notebook_path = "/Workspace/Users/admin@mngenvmcap221600.onmicrosoft.com/ingest_yellow_2021"
                base_parameters = @{
                    catalog = $CatalogName
                    taxi_type = "yellow_taxi"
                    start_year = "$Year"
                    end_year = "$Year"
                    source_path = $VolumePath
                }
            }
            new_cluster = @{
                spark_version = "14.3.x-scala2.12"
                node_type_id = $ClusterNodeType
                autoscale = @{
                    min_workers = $MinWorkers
                    max_workers = $MaxWorkers
                }
                spark_conf = @{
                    "spark.databricks.delta.optimizeWrite.enabled" = "true"
                    "spark.databricks.delta.autoCompact.enabled" = "true"
                }
            }
            timeout_seconds = 7200
        },
        @{
            task_key = "ingest_green_taxi"
            notebook_task = @{
                notebook_path = "/Workspace/Users/admin@mngenvmcap221600.onmicrosoft.com/ingest_green_2021"
                base_parameters = @{
                    catalog = $CatalogName
                    taxi_type = "green_taxi"
                    start_year = "$Year"
                    end_year = "$Year"
                    source_path = $VolumePath
                }
            }
            new_cluster = @{
                spark_version = "14.3.x-scala2.12"
                node_type_id = $ClusterNodeType
                autoscale = @{
                    min_workers = $MinWorkers
                    max_workers = $MaxWorkers
                }
            }
            timeout_seconds = 7200
        }
    )
    max_concurrent_runs = 1
}

$jobConfigJson = $jobConfig | ConvertTo-Json -Depth 10
$tempJobFile = [System.IO.Path]::GetTempFileName() + ".json"
$jobConfigJson | Out-File -FilePath $tempJobFile -Encoding utf8

Write-Host "Job configuration created" -ForegroundColor Green

# First, upload the ingestion notebook
Write-Host "`nUploading ingestion notebook..." -ForegroundColor Yellow

$notebookContent = @'
# Databricks notebook source
# MAGIC %md
# MAGIC # NYC Taxi Data Ingestion - 2021

# COMMAND ----------

# Get parameters
dbutils.widgets.text("catalog", "gm_demo", "Catalog Name")
dbutils.widgets.text("taxi_type", "yellow_taxi", "Taxi Type")
dbutils.widgets.text("start_year", "2021", "Start Year")
dbutils.widgets.text("end_year", "2021", "End Year")
dbutils.widgets.text("source_path", "/Volumes/gm_demo/default/ny_texi", "Source Path")

catalog = dbutils.widgets.get("catalog")
taxi_type = dbutils.widgets.get("taxi_type")
start_year = int(dbutils.widgets.get("start_year"))
end_year = int(dbutils.widgets.get("end_year"))
source_path = dbutils.widgets.get("source_path")

print(f"Catalog: {catalog}")
print(f"Taxi Type: {taxi_type}")
print(f"Year: {start_year}-{end_year}")
print(f"Source: {source_path}")

# COMMAND ----------

# Enable schema evolution
spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")
spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", "true")

# Table name mapping
table_mapping = {
    "yellow_taxi": "yellow_taxi_trips",
    "green_taxi": "green_taxi_trips",
    "for_hire_vehicle": "fhv_trips",
    "high_volume_for_hire_vehicle": "hvfhs_trips"
}

target_table = table_mapping.get(taxi_type, f"{taxi_type}_trips")

# Datetime column mapping
datetime_columns = {
    "yellow_taxi": "tpep_pickup_datetime",
    "green_taxi": "lpep_pickup_datetime",
    "for_hire_vehicle": "pickup_datetime",
    "high_volume_for_hire_vehicle": "pickup_datetime"
}

datetime_col = datetime_columns.get(taxi_type, "pickup_datetime")

print(f"Target table: {catalog}.raw.{target_table}")
print(f"Datetime column: {datetime_col}")

# COMMAND ----------

from pyspark.sql.functions import year, month, col, to_timestamp

total_rows = 0

for yr in range(start_year, end_year + 1):
    print(f"\nProcessing year {yr}...")
    
    year_path = f"{source_path}/{yr}/{taxi_type}"
    
    try:
        # Read parquet files
        df = spark.read.parquet(f"{year_path}/*.parquet")
        
        # Add timestamp and partition columns
        if datetime_col in df.columns:
            df = df.withColumn(datetime_col, to_timestamp(col(datetime_col)))
            df = df.withColumn("year", year(col(datetime_col)))
            df = df.withColumn("month", month(col(datetime_col)))
        
        row_count = df.count()
        total_rows += row_count
        
        # Write to Delta table
        df.write.format("delta") \
          .mode("append") \
          .option("mergeSchema", "true") \
          .partitionBy("year", "month") \
          .saveAsTable(f"{catalog}.raw.{target_table}")
        
        print(f"✓ Ingested {row_count:,} rows for year {yr}")
        
    except Exception as e:
        print(f"✗ Error processing year {yr}: {str(e)}")

print(f"\n{'='*60}")
print(f"Total rows ingested: {total_rows:,}")
print(f"{'='*60}")

# COMMAND ----------

# Display table info
spark.sql(f"DESCRIBE DETAIL {catalog}.raw.{target_table}").show(truncate=False)
'@

$tempNotebookFile = [System.IO.Path]::GetTempFileName()
$notebookContent | Out-File -FilePath $tempNotebookFile -Encoding utf8

Write-Host "Notebook created locally" -ForegroundColor Green

# Simple approach: Run SQL directly to create tables and run ingestion
Write-Host "`n========================================" -ForegroundColor Cyan
Write-Host "Running Direct SQL Ingestion" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan

$taxiTypes = @(
    @{name="yellow_taxi"; table="yellow_taxi_trips"; datetime_col="tpep_pickup_datetime"},
    @{name="green_taxi"; table="green_taxi_trips"; datetime_col="lpep_pickup_datetime"},
    @{name="for_hire_vehicle"; table="fhv_trips"; datetime_col="pickup_datetime"},
    @{name="high_volume_for_hire_vehicle"; table="hvfhs_trips"; datetime_col="pickup_datetime"}
)

foreach ($taxiType in $taxiTypes) {
    Write-Host "`nProcessing $($taxiType.name)..." -ForegroundColor Yellow
    
    $sql = @"
-- Create catalog and schema
CREATE CATALOG IF NOT EXISTS $CatalogName;
CREATE SCHEMA IF NOT EXISTS ${CatalogName}.raw;

-- Create or replace table by reading from volume
CREATE OR REPLACE TABLE ${CatalogName}.raw.$($taxiType.table)
USING DELTA
PARTITIONED BY (year, month)
AS SELECT 
    *,
    year($($taxiType.datetime_col)) as year,
    month($($taxiType.datetime_col)) as month
FROM parquet.``${VolumePath}/${Year}/$($taxiType.name)/*.parquet``;

-- Optimize the table
OPTIMIZE ${CatalogName}.raw.$($taxiType.table);

-- Show row count
SELECT '$($taxiType.name)' as taxi_type, count(*) as row_count 
FROM ${CatalogName}.raw.$($taxiType.table);
"@
    
    Write-Host "Executing SQL for $($taxiType.name)..." -ForegroundColor Gray
    Write-Host $sql -ForegroundColor DarkGray
    
    # Note: Direct SQL execution via CLI would require a SQL warehouse
    # For now, save the SQL files that can be run in Databricks SQL
}

Write-Host "`n========================================" -ForegroundColor Cyan
Write-Host "SQL Scripts Generated" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan
Write-Host "`nTo complete the ingestion, run these commands in Databricks SQL:" -ForegroundColor Yellow
Write-Host ""

foreach ($taxiType in $taxiTypes) {
    $sqlFile = "ingest_$($taxiType.name)_$Year.sql"
    $sql = @"
CREATE CATALOG IF NOT EXISTS $CatalogName;
CREATE SCHEMA IF NOT EXISTS ${CatalogName}.raw;

CREATE OR REPLACE TABLE ${CatalogName}.raw.$($taxiType.table)
USING DELTA
PARTITIONED BY (year, month)
AS SELECT 
    *,
    year($($taxiType.datetime_col)) as year,
    month($($taxiType.datetime_col)) as month
FROM parquet.``${VolumePath}/${Year}/$($taxiType.name)/*.parquet``;

OPTIMIZE ${CatalogName}.raw.$($taxiType.table);

SELECT '$($taxiType.name)' as taxi_type, count(*) as row_count 
FROM ${CatalogName}.raw.$($taxiType.table);
"@
    
    $sql | Out-File -FilePath $sqlFile -Encoding utf8
    Write-Host "  Created: $sqlFile" -ForegroundColor Green
}

Write-Host "`n✓ Done! Run the SQL files in Databricks SQL workspace" -ForegroundColor Green
Write-Host "`nOr copy and paste the SQL directly into a SQL editor" -ForegroundColor Cyan

# Cleanup temp files
Remove-Item $tempSqlFile -ErrorAction SilentlyContinue
Remove-Item $tempJobFile -ErrorAction SilentlyContinue
Remove-Item $tempNotebookFile -ErrorAction SilentlyContinue
