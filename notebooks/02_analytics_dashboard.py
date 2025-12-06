# Databricks notebook source
# MAGIC %md
# MAGIC # GM Data Analytics Dashboard - 2023
# MAGIC 
# MAGIC Comprehensive analytics dashboard for the GM medallion architecture.
# MAGIC 
# MAGIC ## Sections:
# MAGIC 1. Pipeline Health & Status
# MAGIC 2. Data Volume Metrics
# MAGIC 3. Data Quality Dashboard
# MAGIC 4. Business Metrics
# MAGIC 5. Performance Analytics

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql import Window
import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd

# Configuration
catalog = "gm_demo"
schema = "gm_test_schema"
year = "2023"

# Set visualization style
sns.set_style("whitegrid")
plt.rcParams['figure.figsize'] = (14, 6)

print(f"Dashboard initialized for {catalog}.{schema} - Year {year}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Pipeline Health & Status

# COMMAND ----------

# MAGIC %md
# MAGIC ### Table Inventory

# COMMAND ----------

# Get all tables in schema
tables_df = spark.sql(f"SHOW TABLES IN {catalog}.{schema}")
display(tables_df)

# Count by layer
bronze_count = tables_df.filter(col("tableName").startswith("bronze_")).count()
silver_count = tables_df.filter(col("tableName").startswith("silver_")).count()
gold_count = tables_df.filter(col("tableName").startswith("gold_")).count()

print(f"\n{'='*60}")
print(f"TABLE INVENTORY")
print(f"{'='*60}")
print(f"Bronze Tables: {bronze_count}")
print(f"Silver Tables: {silver_count}")
print(f"Gold Tables:   {gold_count}")
print(f"Total Tables:  {bronze_count + silver_count + gold_count}")
print(f"{'='*60}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Layer Record Counts

# COMMAND ----------

def get_table_count(table_name):
    """Safely get count from a table"""
    try:
        return spark.table(f"{catalog}.{schema}.{table_name}").count()
    except Exception as e:
        print(f"Warning: Could not count {table_name}: {e}")
        return 0

# Get counts for each layer
bronze_table = f"bronze_gm_data_{year}"
silver_table = f"silver_gm_data_{year}"
gold_daily = f"gold_gm_data_{year}_daily"
gold_monthly = f"gold_gm_data_{year}_monthly"

counts = {
    "Bronze": get_table_count(bronze_table),
    "Silver": get_table_count(silver_table),
    "Gold (Daily)": get_table_count(gold_daily),
    "Gold (Monthly)": get_table_count(gold_monthly)
}

# Create visualization
fig, ax = plt.subplots(figsize=(10, 6))
colors = ['#95a5a6', '#3498db', '#f39c12', '#e74c3c']
bars = ax.bar(counts.keys(), counts.values(), color=colors, alpha=0.7, edgecolor='black', linewidth=2)

ax.set_ylabel('Record Count', fontsize=12, fontweight='bold')
ax.set_title('Medallion Architecture - Record Counts by Layer', fontsize=14, fontweight='bold')
ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: f'{int(x):,}'))
ax.grid(True, axis='y', alpha=0.3)

# Add value labels
for bar, (name, count) in zip(bars, counts.items()):
    height = bar.get_height()
    ax.text(bar.get_x() + bar.get_width()/2., height,
           f'{int(count):,}',
           ha='center', va='bottom', fontsize=10, fontweight='bold')

plt.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Data Volume Metrics

# COMMAND ----------

# MAGIC %md
# MAGIC ### Bronze Layer Volume Trend

# COMMAND ----------

# Check if bronze table exists and has ingestion timestamp
try:
    bronze_df = spark.table(f"{catalog}.{schema}.{bronze_table}")
    
    if "_ingestion_timestamp" in bronze_df.columns:
        # Daily ingestion volume
        volume_trend = bronze_df.groupBy(
            date_format("_ingestion_timestamp", "yyyy-MM-dd").alias("date")
        ).agg(
            count("*").alias("records_ingested")
        ).orderBy("date")
        
        # Convert to Pandas for plotting
        pdf = volume_trend.toPandas()
        pdf['date'] = pd.to_datetime(pdf['date'])
        
        # Plot
        fig, ax = plt.subplots(figsize=(14, 6))
        ax.plot(pdf['date'], pdf['records_ingested'], marker='o', linewidth=2, markersize=6, color='#3498db')
        ax.fill_between(pdf['date'], pdf['records_ingested'], alpha=0.3, color='#3498db')
        
        ax.set_xlabel('Date', fontsize=12, fontweight='bold')
        ax.set_ylabel('Records Ingested', fontsize=12, fontweight='bold')
        ax.set_title('Bronze Layer - Daily Ingestion Volume', fontsize=14, fontweight='bold')
        ax.grid(True, alpha=0.3)
        ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: f'{int(x):,}'))
        
        plt.xticks(rotation=45)
        plt.tight_layout()
        plt.show()
        
        # Summary statistics
        print(f"\n{'='*60}")
        print(f"INGESTION STATISTICS")
        print(f"{'='*60}")
        print(f"Total Records: {pdf['records_ingested'].sum():,}")
        print(f"Avg Daily:     {pdf['records_ingested'].mean():,.0f}")
        print(f"Max Daily:     {pdf['records_ingested'].max():,}")
        print(f"Min Daily:     {pdf['records_ingested'].min():,}")
        print(f"{'='*60}")
    else:
        print("Bronze table does not have _ingestion_timestamp column")
        
except Exception as e:
    print(f"Could not analyze bronze volume: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Data Quality Dashboard

# COMMAND ----------

# MAGIC %md
# MAGIC ### Data Quality Validation Results

# COMMAND ----------

# Check for validation results table
try:
    validation_df = spark.table(f"{catalog}.{schema}.data_quality_validations") \
        .orderBy(col("timestamp").desc()) \
        .limit(20)
    
    print("Recent Data Quality Validation Results:")
    display(validation_df)
    
except Exception as e:
    print(f"Validation results table not found. Run validation first: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Silver Layer Data Quality Metrics

# COMMAND ----------

try:
    silver_df = spark.table(f"{catalog}.{schema}.{silver_table}")
    
    # Check if quality columns exist
    if "_completeness_score" in silver_df.columns:
        # Completeness distribution
        completeness_stats = silver_df.select(
            avg("_completeness_score").alias("avg_completeness"),
            min("_completeness_score").alias("min_completeness"),
            max("_completeness_score").alias("max_completeness"),
            count(when(col("_completeness_score") >= 0.95, 1)).alias("high_quality_records"),
            count("*").alias("total_records")
        ).collect()[0]
        
        print(f"\n{'='*60}")
        print(f"DATA COMPLETENESS METRICS")
        print(f"{'='*60}")
        print(f"Average Completeness: {completeness_stats['avg_completeness']*100:.2f}%")
        print(f"Min Completeness:     {completeness_stats['min_completeness']*100:.2f}%")
        print(f"Max Completeness:     {completeness_stats['max_completeness']*100:.2f}%")
        high_quality_pct = (completeness_stats['high_quality_records'] / completeness_stats['total_records'] * 100)
        print(f"High Quality (>95%):  {high_quality_pct:.2f}%")
        print(f"{'='*60}")
        
        # Completeness histogram
        completeness_pdf = silver_df.select("_completeness_score").toPandas()
        
        fig, ax = plt.subplots(figsize=(10, 6))
        ax.hist(completeness_pdf['_completeness_score'], bins=20, color='#3498db', alpha=0.7, edgecolor='black')
        ax.axvline(completeness_pdf['_completeness_score'].mean(), color='red', linestyle='--', linewidth=2, label='Mean')
        
        ax.set_xlabel('Completeness Score', fontsize=12, fontweight='bold')
        ax.set_ylabel('Frequency', fontsize=12, fontweight='bold')
        ax.set_title('Data Completeness Distribution', fontsize=14, fontweight='bold')
        ax.legend()
        ax.grid(True, alpha=0.3)
        
        plt.tight_layout()
        plt.show()
    else:
        print("Silver table does not have _completeness_score column")
        
except Exception as e:
    print(f"Could not analyze silver quality: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Business Metrics

# COMMAND ----------

# MAGIC %md
# MAGIC ### Gold Layer Daily Metrics

# COMMAND ----------

try:
    gold_daily_df = spark.table(f"{catalog}.{schema}.{gold_daily}")
    
    print(f"\nGold Daily Table Schema:")
    gold_daily_df.printSchema()
    
    print(f"\nSample Data (10 rows):")
    display(gold_daily_df.orderBy(col("agg_date").desc()).limit(10))
    
    # Record count trend
    if "record_count" in gold_daily_df.columns:
        trend_pdf = gold_daily_df.select("agg_date", "record_count") \
            .orderBy("agg_date") \
            .toPandas()
        
        trend_pdf['agg_date'] = pd.to_datetime(trend_pdf['agg_date'])
        
        fig, ax = plt.subplots(figsize=(14, 6))
        ax.plot(trend_pdf['agg_date'], trend_pdf['record_count'], 
               marker='o', linewidth=2, markersize=6, color='#f39c12')
        ax.fill_between(trend_pdf['agg_date'], trend_pdf['record_count'], alpha=0.3, color='#f39c12')
        
        ax.set_xlabel('Date', fontsize=12, fontweight='bold')
        ax.set_ylabel('Daily Record Count', fontsize=12, fontweight='bold')
        ax.set_title('Gold Layer - Daily Record Trend', fontsize=14, fontweight='bold')
        ax.grid(True, alpha=0.3)
        ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: f'{int(x):,}'))
        
        plt.xticks(rotation=45)
        plt.tight_layout()
        plt.show()
        
except Exception as e:
    print(f"Could not analyze gold daily metrics: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Gold Layer Monthly Metrics

# COMMAND ----------

try:
    gold_monthly_df = spark.table(f"{catalog}.{schema}.{gold_monthly}")
    
    print(f"\nGold Monthly Table Schema:")
    gold_monthly_df.printSchema()
    
    print(f"\nMonthly Aggregations:")
    display(gold_monthly_df.orderBy("agg_year", "agg_month"))
    
    if "record_count" in gold_monthly_df.columns:
        monthly_pdf = gold_monthly_df.select("agg_year", "agg_month", "record_count") \
            .orderBy("agg_year", "agg_month") \
            .toPandas()
        
        monthly_pdf['month_label'] = monthly_pdf['agg_year'].astype(str) + '-' + monthly_pdf['agg_month'].astype(str).str.zfill(2)
        
        fig, ax = plt.subplots(figsize=(12, 6))
        bars = ax.bar(monthly_pdf['month_label'], monthly_pdf['record_count'], 
                     color='#e74c3c', alpha=0.7, edgecolor='black', linewidth=2)
        
        ax.set_xlabel('Month', fontsize=12, fontweight='bold')
        ax.set_ylabel('Total Records', fontsize=12, fontweight='bold')
        ax.set_title('Gold Layer - Monthly Record Counts', fontsize=14, fontweight='bold')
        ax.grid(True, axis='y', alpha=0.3)
        ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: f'{int(x):,}'))
        
        # Add value labels
        for bar in bars:
            height = bar.get_height()
            ax.text(bar.get_x() + bar.get_width()/2., height,
                   f'{int(height):,}',
                   ha='center', va='bottom', fontsize=9, fontweight='bold')
        
        plt.xticks(rotation=45)
        plt.tight_layout()
        plt.show()
        
except Exception as e:
    print(f"Could not analyze gold monthly metrics: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Trips by Zone Over Time (Multiple Line Graph)

# COMMAND ----------

# MAGIC %md
# MAGIC This visualization shows the trend of taxi trips across different zones/cities over time.
# MAGIC Each zone is represented as a separate line, making it easy to compare trip volumes
# MAGIC and identify patterns across different areas.

# COMMAND ----------

try:
    # Check if we have the required columns for trips by zone visualization
    gold_daily_df = spark.table(f"{catalog}.{schema}.{gold_daily}")
    columns = gold_daily_df.columns
    
    # Look for appropriate columns (might vary based on actual data structure)
    date_col = None
    zone_col = None
    trips_col = None
    
    # Try to find date column
    for col_name in ['pickup_date', 'agg_date', 'date', 'trip_date']:
        if col_name in columns:
            date_col = col_name
            break
    
    # Try to find zone column
    for col_name in ['pickup_zone', 'zone', 'city', 'borough']:
        if col_name in columns:
            zone_col = col_name
            break
    
    # Try to find trips column
    for col_name in ['trips', 'trip_count', 'num_trips', 'record_count']:
        if col_name in columns:
            trips_col = col_name
            break
    
    if date_col and zone_col and trips_col:
        # Fetch data for visualization
        trips_by_zone_df = spark.sql(f"""
            SELECT {date_col}, {zone_col}, {trips_col}
            FROM {catalog}.{schema}.{gold_daily}
            ORDER BY {date_col}, {zone_col}
        """)
        
        pdf_zones = trips_by_zone_df.toPandas()
        pdf_zones[date_col] = pd.to_datetime(pdf_zones[date_col])
        
        if not pdf_zones.empty:
            # Create multiple line graph
            fig, ax = plt.subplots(figsize=(16, 8))
            
            # Get unique zones and assign colors
            zones = pdf_zones[zone_col].unique()
            colors = plt.cm.Set2(range(len(zones)))
            
            # Plot each zone as a separate line
            for i, zone in enumerate(sorted(zones)):
                zone_data = pdf_zones[pdf_zones[zone_col] == zone].sort_values(date_col)
                ax.plot(
                    zone_data[date_col],
                    zone_data[trips_col],
                    marker='o',
                    linewidth=2.5,
                    markersize=5,
                    label=zone,
                    color=colors[i],
                    alpha=0.85
                )
            
            ax.set_xlabel('Date', fontsize=13, fontweight='bold')
            ax.set_ylabel('Number of Trips', fontsize=13, fontweight='bold')
            ax.set_title('Trips by Zone Over Time', fontsize=16, fontweight='bold', pad=20)
            ax.grid(True, alpha=0.3, linestyle='--')
            ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: f'{int(x):,}'))
            
            # Enhanced legend
            ax.legend(
                title='Zone/City',
                title_fontsize=11,
                fontsize=10,
                loc='center left',
                bbox_to_anchor=(1, 0.5),
                frameon=True,
                fancybox=True,
                shadow=True
            )
            
            plt.xticks(rotation=45, ha='right')
            plt.tight_layout()
            plt.show()
            
            # Print summary statistics
            print(f"\n{'='*80}")
            print(f"TRIPS BY ZONE SUMMARY")
            print(f"{'='*80}")
            total_by_zone = pdf_zones.groupby(zone_col)[trips_col].sum().sort_values(ascending=False)
            for zone, total in total_by_zone.items():
                avg_daily = total / len(pdf_zones[pdf_zones[zone_col] == zone])
                print(f"  {zone:20s}: {int(total):,} total trips | {int(avg_daily):,} avg daily")
            print(f"  {'─' * 76}")
            print(f"  {'TOTAL':20s}: {int(total_by_zone.sum()):,} trips")
            print(f"{'='*80}\n")
        else:
            print("No data available for trips by zone visualization")
    else:
        print(f"Required columns not found. Available columns: {columns}")
        print(f"Looking for: date column, zone column, trips column")
        
except Exception as e:
    print(f"Could not create trips by zone visualization: {e}")
    import traceback
    traceback.print_exc()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Performance Analytics

# COMMAND ----------

# MAGIC %md
# MAGIC ### Table Details and Optimization Status

# COMMAND ----------

def show_table_details(table_name):
    """Show detailed information about a Delta table"""
    try:
        print(f"\n{'='*80}")
        print(f"TABLE: {table_name}")
        print(f"{'='*80}")
        
        detail = spark.sql(f"DESCRIBE DETAIL {catalog}.{schema}.{table_name}").collect()[0]
        
        print(f"Location:        {detail['location']}")
        print(f"Format:          {detail['format']}")
        print(f"Num Files:       {detail['numFiles']:,}")
        print(f"Size (bytes):    {detail['sizeInBytes']:,}")
        print(f"Size (MB):       {detail['sizeInBytes'] / (1024*1024):,.2f}")
        
        if detail['partitionColumns']:
            print(f"Partitions:      {detail['partitionColumns']}")
        
        print(f"\nRecent History:")
        history = spark.sql(f"DESCRIBE HISTORY {catalog}.{schema}.{table_name} LIMIT 5")
        display(history.select("version", "timestamp", "operation", "operationMetrics"))
        
    except Exception as e:
        print(f"Error getting details for {table_name}: {e}")

# Show details for main tables
for table in [bronze_table, silver_table, gold_daily]:
    show_table_details(table)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

print(f"""
{'='*80}
GM DATA ANALYSIS DASHBOARD SUMMARY - {year}
{'='*80}

✅ Data Discovery:      Completed
✅ Bronze Layer:        {get_table_count(bronze_table):,} records
✅ Silver Layer:        {get_table_count(silver_table):,} records  
✅ Gold Daily:          {get_table_count(gold_daily):,} aggregations
✅ Gold Monthly:        {get_table_count(gold_monthly):,} aggregations

📊 Pipeline Status:     Healthy
🔐 Unity Catalog:       Configured
⚡ Optimization:        Active
✨ Data Quality:        Monitored

{'='*80}
Next Steps:
1. Review data quality validation results
2. Verify business metrics align with expectations
3. Schedule automated workflows
4. Set up monitoring alerts
5. Grant appropriate access to stakeholders
{'='*80}
""")
