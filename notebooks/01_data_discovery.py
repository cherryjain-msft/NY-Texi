# Databricks notebook source
# MAGIC %md
# MAGIC # GM Data Discovery - 2023
# MAGIC 
# MAGIC This notebook explores the data in `/Volumes/gm_demo/gm_test_schema/gm_volume/2023/`
# MAGIC 
# MAGIC ## Objectives:
# MAGIC 1. List all files in the volume
# MAGIC 2. Identify data formats
# MAGIC 3. Analyze schema structure
# MAGIC 4. Assess data quality
# MAGIC 5. Determine partitioning strategy

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Explore Volume Contents

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import os

# Define the base path
volume_path = "/Volumes/gm_demo/gm_test_schema/gm_volume/2023"

# List all files in the volume
files = dbutils.fs.ls(volume_path)

# Display file information
print(f"Found {len(files)} items in {volume_path}")
print("\nFile/Folder Details:")
print("-" * 80)

for file_info in files:
    file_type = "DIR" if file_info.isDir() else "FILE"
    size_mb = file_info.size / (1024 * 1024) if file_info.size > 0 else 0
    print(f"{file_type:6} | {file_info.name:40} | {size_mb:10.2f} MB")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Identify Data Formats and File Counts

# COMMAND ----------

import os
from collections import defaultdict

def explore_directory(path, max_depth=3, current_depth=0):
    """Recursively explore directory and categorize files"""
    file_stats = defaultdict(lambda: {"count": 0, "total_size": 0, "paths": []})
    
    if current_depth >= max_depth:
        return file_stats
    
    try:
        items = dbutils.fs.ls(path)
        
        for item in items:
            if item.isDir():
                # Recursively explore subdirectories
                sub_stats = explore_directory(item.path, max_depth, current_depth + 1)
                for ext, stats in sub_stats.items():
                    file_stats[ext]["count"] += stats["count"]
                    file_stats[ext]["total_size"] += stats["total_size"]
                    file_stats[ext]["paths"].extend(stats["paths"])
            else:
                # Get file extension
                ext = os.path.splitext(item.name)[1].lower() or "no_extension"
                file_stats[ext]["count"] += 1
                file_stats[ext]["total_size"] += item.size
                if len(file_stats[ext]["paths"]) < 5:  # Store first 5 examples
                    file_stats[ext]["paths"].append(item.path)
    except Exception as e:
        print(f"Error exploring {path}: {e}")
    
    return file_stats

# Explore the volume
print("Analyzing file structure...")
file_statistics = explore_directory(volume_path)

# Display statistics
print("\n" + "=" * 80)
print("FILE FORMAT ANALYSIS")
print("=" * 80)

for ext, stats in sorted(file_statistics.items(), key=lambda x: x[1]["count"], reverse=True):
    size_mb = stats["total_size"] / (1024 * 1024)
    print(f"\n{ext.upper():20} | Count: {stats['count']:6} | Total Size: {size_mb:10.2f} MB")
    print(f"  Sample files:")
    for path in stats["paths"][:3]:
        print(f"    - {path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Sample Data and Schema Analysis

# COMMAND ----------

def analyze_sample_file(file_path, file_format):
    """Analyze a sample file to understand schema and data"""
    print(f"\n{'=' * 80}")
    print(f"Analyzing: {file_path}")
    print(f"Format: {file_format}")
    print('=' * 80)
    
    try:
        # Read sample data based on format
        if file_format in ['.parquet', '.parq']:
            df = spark.read.parquet(file_path)
        elif file_format in ['.csv']:
            df = spark.read.option("header", "true").option("inferSchema", "true").csv(file_path)
        elif file_format in ['.json']:
            df = spark.read.json(file_path)
        else:
            print(f"Unsupported format: {file_format}")
            return None
        
        # Display schema
        print("\nSchema:")
        df.printSchema()
        
        # Display sample data
        print("\nSample Data (5 rows):")
        df.show(5, truncate=False)
        
        # Display statistics
        print("\nData Statistics:")
        print(f"  Total Rows: {df.count():,}")
        print(f"  Total Columns: {len(df.columns)}")
        
        # Column statistics
        print("\nColumn Analysis:")
        for col_name in df.columns:
            null_count = df.filter(col(col_name).isNull()).count()
            null_pct = (null_count / df.count() * 100) if df.count() > 0 else 0
            distinct_count = df.select(col_name).distinct().count()
            
            print(f"  {col_name:30} | Nulls: {null_count:8} ({null_pct:5.2f}%) | Distinct: {distinct_count:8}")
        
        return df
    
    except Exception as e:
        print(f"Error analyzing file: {e}")
        return None

# Analyze sample files for each format
for ext, stats in file_statistics.items():
    if stats["paths"] and ext in ['.parquet', '.parq', '.csv', '.json']:
        sample_file = stats["paths"][0]
        analyze_sample_file(sample_file, ext)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Data Quality Assessment

# COMMAND ----------

def assess_data_quality(df, dataset_name):
    """Comprehensive data quality assessment"""
    print(f"\n{'=' * 80}")
    print(f"DATA QUALITY ASSESSMENT: {dataset_name}")
    print('=' * 80)
    
    total_rows = df.count()
    print(f"\nTotal Records: {total_rows:,}")
    
    # Check for duplicates
    distinct_rows = df.distinct().count()
    duplicate_count = total_rows - distinct_rows
    print(f"Duplicate Records: {duplicate_count:,} ({duplicate_count/total_rows*100:.2f}%)")
    
    # Null analysis by column
    print("\nNull Value Analysis:")
    print(f"{'Column':30} | {'Null Count':>12} | {'Null %':>8} | {'Data Type':15}")
    print("-" * 80)
    
    for col_name, col_type in df.dtypes:
        null_count = df.filter(col(col_name).isNull()).count()
        null_pct = (null_count / total_rows * 100) if total_rows > 0 else 0
        print(f"{col_name:30} | {null_count:12,} | {null_pct:7.2f}% | {col_type:15}")
    
    # Identify potential key columns (low null rate, high cardinality)
    print("\n\nPotential Key Columns (high cardinality, low nulls):")
    for col_name in df.columns:
        null_count = df.filter(col(col_name).isNull()).count()
        null_pct = (null_count / total_rows * 100) if total_rows > 0 else 0
        distinct_count = df.select(col_name).distinct().count()
        cardinality_pct = (distinct_count / total_rows * 100) if total_rows > 0 else 0
        
        if null_pct < 5 and cardinality_pct > 80:
            print(f"  - {col_name}: {distinct_count:,} distinct values ({cardinality_pct:.2f}% cardinality)")

# Run quality assessment on discovered datasets
# This will be updated after we identify the actual data files

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Recommended Partitioning Strategy

# COMMAND ----------

def recommend_partitioning(df, dataset_name):
    """Recommend partitioning strategy based on data characteristics"""
    print(f"\n{'=' * 80}")
    print(f"PARTITIONING RECOMMENDATIONS: {dataset_name}")
    print('=' * 80)
    
    # Look for date/timestamp columns
    date_cols = [col_name for col_name, col_type in df.dtypes 
                 if 'date' in col_type.lower() or 'timestamp' in col_type.lower()]
    
    print("\nIdentified Date/Timestamp Columns:")
    for col_name in date_cols:
        print(f"  - {col_name}")
        min_val = df.agg(min(col(col_name))).collect()[0][0]
        max_val = df.agg(max(col(col_name))).collect()[0][0]
        print(f"    Range: {min_val} to {max_val}")
    
    # Recommend partitioning
    print("\nRecommended Partitioning Strategy:")
    if date_cols:
        print(f"  1. Partition by date column: {date_cols[0]}")
        print(f"     - Use year/month partitioning for large datasets")
        print(f"     - Use daily partitioning for smaller datasets with daily loads")
    
    # Look for categorical columns with reasonable cardinality
    print("\n  2. Consider Z-ordering on frequently filtered columns:")
    for col_name in df.columns:
        distinct_count = df.select(col_name).distinct().count()
        if 10 < distinct_count < 1000:
            print(f"     - {col_name} (cardinality: {distinct_count})")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Summary and Next Steps

# COMMAND ----------

# MAGIC %md
# MAGIC ### Summary
# MAGIC 
# MAGIC Based on the data discovery:
# MAGIC 
# MAGIC 1. **Data Formats Identified**: Review the file format analysis above
# MAGIC 2. **Schema Structure**: Check the schema analysis for each dataset
# MAGIC 3. **Data Quality**: Review null counts and duplicate records
# MAGIC 4. **Volume**: Total size and row counts documented
# MAGIC 
# MAGIC ### Next Steps
# MAGIC 
# MAGIC 1. **Bronze Layer**: Ingest raw data with metadata columns
# MAGIC 2. **Silver Layer**: Clean and deduplicate data
# MAGIC 3. **Gold Layer**: Create business aggregates
# MAGIC 4. **Optimization**: Implement partitioning and Z-ordering
# MAGIC 5. **Validation**: Set up data quality checks
# MAGIC 
# MAGIC ### Action Items
# MAGIC 
# MAGIC - [ ] Review discovered data formats and schemas
# MAGIC - [ ] Validate sample data with business requirements
# MAGIC - [ ] Confirm partitioning strategy
# MAGIC - [ ] Proceed with Bronze layer implementation
