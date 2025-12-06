"""
Example: Using the Trips by Zone Visualization

This script demonstrates different ways to use the trips by zone visualization feature.
"""

# Example 1: Using the standalone script from command line
print("=" * 80)
print("Example 1: Command Line Usage")
print("=" * 80)
print("""
# Basic usage with sample data
python create_trips_by_zone_visualization.py --use-sample

# Save to file
python create_trips_by_zone_visualization.py --use-sample --save my_chart.png

# Custom date range
python create_trips_by_zone_visualization.py --use-sample \\
    --start-date 2023-06-01 \\
    --end-date 2023-08-31
""")

# Example 2: Using in Python code (requires PySpark)
print("\n" + "=" * 80)
print("Example 2: Python Code with Databricks")
print("=" * 80)
print("""
from pyspark.sql import SparkSession
from src.gm_analysis.analytics.visualize_metrics import create_trips_by_zone_chart

# Initialize Spark
spark = SparkSession.builder.getOrCreate()

# Create visualization
create_trips_by_zone_chart(
    spark=spark,
    catalog="gm_demo",
    schema="gm_test_schema",
    table_name="gold_gm_data_2023_daily",
    title="NYC Taxi Trips by Zone - 2023"
)
""")

# Example 3: Using the class directly
print("\n" + "=" * 80)
print("Example 3: Using AnalyticsVisualizer Class")
print("=" * 80)
print("""
from pyspark.sql import SparkSession
from src.gm_analysis.analytics.visualize_metrics import AnalyticsVisualizer

spark = SparkSession.builder.getOrCreate()

# Create visualizer
viz = AnalyticsVisualizer(
    spark=spark,
    catalog="gm_demo",
    schema="gm_test_schema"
)

# Create trips by zone chart
viz.plot_trips_by_zone(
    table_name="gold_gm_data_2023_daily",
    date_column="pickup_date",
    zone_column="pickup_zone",
    trips_column="trips",
    title="Trips by Zone Over Time"
)

# You can also create other visualizations
viz.plot_data_volume_trends(
    table_name="bronze_gm_data_2023",
    date_column="_ingestion_timestamp"
)

viz.plot_layer_comparison(
    bronze_table="bronze_gm_data_2023",
    silver_table="silver_gm_data_2023",
    gold_table="gold_gm_data_2023_daily"
)
""")

# Example 4: Generating sample data programmatically
print("\n" + "=" * 80)
print("Example 4: Generate Sample Data in Code")
print("=" * 80)
print("""
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

# Generate dates
dates = pd.date_range('2023-01-01', '2023-12-31', freq='D')
zones = ["Manhattan", "Brooklyn", "Queens", "Bronx", "Staten Island", "JFK", "LGA"]

data = []
for date in dates:
    for zone in zones:
        trips = np.random.randint(500, 8000)  # Random trip count
        data.append({
            'pickup_date': date,
            'pickup_zone': zone,
            'trips': trips
        })

df = pd.DataFrame(data)

# Now create visualization using pandas
from create_trips_by_zone_visualization import create_trips_by_zone_chart_pandas

create_trips_by_zone_chart_pandas(
    df=df,
    title="My Custom Trips Analysis",
    save_path="custom_chart.png"
)
""")

# Example 5: Integration with analytics dashboard
print("\n" + "=" * 80)
print("Example 5: In Databricks Analytics Dashboard")
print("=" * 80)
print("""
# The visualization is automatically included in the analytics dashboard
# Run the dashboard notebook:

databricks notebook run notebooks/02_analytics_dashboard.py

# Or open it in Databricks UI and run all cells
# The 'Trips by Zone Over Time' section will appear under Business Metrics
""")

print("\n" + "=" * 80)
print("Quick Reference")
print("=" * 80)
print("""
Key Files:
- create_trips_by_zone_visualization.py : Standalone script
- src/gm_analysis/analytics/visualize_metrics.py : Core module
- notebooks/02_analytics_dashboard.py : Dashboard with visualization
- TRIPS_BY_ZONE_VISUALIZATION.md : Complete documentation

Quick Commands:
- Generate sample visualization: 
  python create_trips_by_zone_visualization.py --use-sample
  
- Save to file: 
  python create_trips_by_zone_visualization.py --use-sample --save chart.png
  
- Custom dates: 
  python create_trips_by_zone_visualization.py --use-sample \\
    --start-date 2024-01-01 --end-date 2024-03-31
""")

print("\n✓ See TRIPS_BY_ZONE_VISUALIZATION.md for complete documentation")
