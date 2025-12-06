#!/usr/bin/env python3
"""
Create Multiple Line Graph: Trips by Zone/City Over Time

This script demonstrates the trips by zone visualization using synthetic NYC taxi data.
It can work with either real data from the gold layer or generate sample data for demonstration.

Usage:
    # With real Databricks data:
    python create_trips_by_zone_visualization.py --use-databricks --year 2023
    
    # With sample data (no Databricks required):
    python create_trips_by_zone_visualization.py --use-sample
"""

import argparse
import sys
from datetime import datetime, timedelta
import random

# Check if running in Databricks environment
try:
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import col
    PYSPARK_AVAILABLE = True
except ImportError:
    PYSPARK_AVAILABLE = False
    print("Note: PySpark not available. Using sample data mode only.")

import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd
import numpy as np

# Set style
sns.set_style("whitegrid")
plt.rcParams['figure.figsize'] = (16, 8)


def generate_sample_data(start_date='2023-01-01', end_date='2023-12-31', seed=42):
    """
    Generate sample taxi trip data for demonstration purposes.
    
    Args:
        start_date: Start date for data generation
        end_date: End date for data generation
        seed: Random seed for reproducibility
    
    Returns:
        pandas DataFrame with columns: pickup_date, pickup_zone, trips
    """
    random.seed(seed)
    np.random.seed(seed)
    
    # NYC zones/boroughs
    zones = ["Manhattan", "Brooklyn", "Queens", "Bronx", "Staten Island", "JFK", "LGA"]
    
    # Generate date range
    start = pd.to_datetime(start_date)
    end = pd.to_datetime(end_date)
    dates = pd.date_range(start, end, freq='D')
    
    data = []
    
    # Base trip counts per zone (daily average)
    base_trips = {
        "Manhattan": 5000,
        "Brooklyn": 3000,
        "Queens": 2500,
        "Bronx": 1500,
        "Staten Island": 500,
        "JFK": 1200,
        "LGA": 1000
    }
    
    for date in dates:
        # Add day-of-week variation (weekends are busier)
        day_of_week = date.dayofweek
        weekend_multiplier = 1.25 if day_of_week >= 5 else 1.0
        
        # Add monthly seasonality (summer busier)
        month = date.month
        seasonal_multiplier = 1.3 if month in [6, 7, 8] else 0.9 if month in [1, 2] else 1.0
        
        for zone in zones:
            # Calculate trips with variations
            base = base_trips[zone]
            trips = int(base * weekend_multiplier * seasonal_multiplier * 
                       (1 + np.random.normal(0, 0.15)))  # Add random variation
            trips = max(trips, 0)  # Ensure non-negative
            
            data.append({
                'pickup_date': date,
                'pickup_zone': zone,
                'trips': trips
            })
    
    df = pd.DataFrame(data)
    return df


def create_trips_by_zone_chart_pandas(df, title=None, save_path=None):
    """
    Create multiple line graph using pandas DataFrame.
    
    Args:
        df: DataFrame with columns: pickup_date, pickup_zone, trips
        title: Optional custom title
        save_path: Optional path to save the figure
    """
    # Ensure date column is datetime
    df['pickup_date'] = pd.to_datetime(df['pickup_date'])
    
    # Create figure
    fig, ax = plt.subplots(figsize=(16, 8))
    
    # Get unique zones and assign colors
    zones = sorted(df['pickup_zone'].unique())
    colors = plt.cm.Set2(range(len(zones)))
    
    # Plot each zone as a separate line
    for i, zone in enumerate(zones):
        zone_data = df[df['pickup_zone'] == zone].sort_values('pickup_date')
        ax.plot(
            zone_data['pickup_date'],
            zone_data['trips'],
            marker='o',
            linewidth=2.5,
            markersize=5,
            label=zone,
            color=colors[i],
            alpha=0.85
        )
    
    ax.set_xlabel('Date', fontsize=13, fontweight='bold')
    ax.set_ylabel('Number of Trips', fontsize=13, fontweight='bold')
    ax.set_title(
        title or 'NYC Taxi Trips by Zone Over Time',
        fontsize=16,
        fontweight='bold',
        pad=20
    )
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
    
    if save_path:
        plt.savefig(save_path, dpi=300, bbox_inches='tight')
        print(f"Chart saved to: {save_path}")
    
    plt.show()
    
    # Print summary statistics
    print(f"\n{'='*80}")
    print(f"TRIPS BY ZONE SUMMARY")
    print(f"{'='*80}")
    total_by_zone = df.groupby('pickup_zone')['trips'].sum().sort_values(ascending=False)
    for zone, total in total_by_zone.items():
        avg_daily = total / len(df[df['pickup_zone'] == zone])
        print(f"  {zone:20s}: {int(total):,} total trips | {int(avg_daily):,} avg daily")
    print(f"  {'─' * 76}")
    print(f"  {'TOTAL':20s}: {int(total_by_zone.sum()):,} trips")
    print(f"{'='*80}\n")


def create_trips_by_zone_chart_spark(
    spark,
    catalog="gm_demo",
    schema="gm_test_schema",
    table_name="gold_gm_data_2023_daily",
    date_column="pickup_date",
    zone_column="pickup_zone",
    trips_column="trips",
    title=None,
    save_path=None
):
    """
    Create multiple line graph using PySpark and gold layer data.
    
    Args:
        spark: SparkSession
        catalog: Unity Catalog name
        schema: Schema name
        table_name: Gold table name
        date_column: Date column name
        zone_column: Zone column name
        trips_column: Trips column name
        title: Optional custom title
        save_path: Optional path to save the figure
    """
    full_table = f"{catalog}.{schema}.{table_name}"
    
    print(f"Reading data from: {full_table}")
    
    # Query data
    df = spark.sql(f"""
        SELECT {date_column}, {zone_column}, {trips_column}
        FROM {full_table}
        ORDER BY {date_column}, {zone_column}
    """)
    
    # Convert to pandas
    pdf = df.toPandas()
    
    if pdf.empty:
        print(f"No data found in {full_table}")
        return
    
    # Rename columns for consistency
    pdf.columns = ['pickup_date', 'pickup_zone', 'trips']
    
    # Create chart
    create_trips_by_zone_chart_pandas(pdf, title=title, save_path=save_path)


def main():
    parser = argparse.ArgumentParser(
        description="Create multiple line graph showing trips by zone/city over time"
    )
    parser.add_argument(
        '--use-databricks',
        action='store_true',
        help='Use real data from Databricks gold layer'
    )
    parser.add_argument(
        '--use-sample',
        action='store_true',
        help='Use generated sample data (default if neither option specified)'
    )
    parser.add_argument(
        '--year',
        type=str,
        default='2023',
        help='Year for data analysis (default: 2023)'
    )
    parser.add_argument(
        '--catalog',
        type=str,
        default='gm_demo',
        help='Unity Catalog name (default: gm_demo)'
    )
    parser.add_argument(
        '--schema',
        type=str,
        default='gm_test_schema',
        help='Schema name (default: gm_test_schema)'
    )
    parser.add_argument(
        '--save',
        type=str,
        help='Path to save the figure (e.g., trips_by_zone.png)'
    )
    parser.add_argument(
        '--start-date',
        type=str,
        default='2023-01-01',
        help='Start date for sample data (default: 2023-01-01)'
    )
    parser.add_argument(
        '--end-date',
        type=str,
        default='2023-12-31',
        help='End date for sample data (default: 2023-12-31)'
    )
    
    args = parser.parse_args()
    
    # Default to sample data if neither option specified
    if not args.use_databricks and not args.use_sample:
        args.use_sample = True
    
    if args.use_databricks:
        if not PYSPARK_AVAILABLE:
            print("Error: PySpark is not available. Cannot use Databricks mode.")
            print("Please install PySpark or use --use-sample flag.")
            sys.exit(1)
        
        print("Using Databricks data...")
        spark = SparkSession.builder.getOrCreate()
        
        table_name = f"gold_gm_data_{args.year}_daily"
        title = f"NYC Taxi Trips by Zone - {args.year}"
        
        create_trips_by_zone_chart_spark(
            spark=spark,
            catalog=args.catalog,
            schema=args.schema,
            table_name=table_name,
            title=title,
            save_path=args.save
        )
    else:
        print("Generating sample data...")
        df = generate_sample_data(
            start_date=args.start_date,
            end_date=args.end_date
        )
        
        print(f"Generated {len(df)} records")
        print(f"Date range: {df['pickup_date'].min()} to {df['pickup_date'].max()}")
        print(f"Zones: {', '.join(sorted(df['pickup_zone'].unique()))}\n")
        
        title = f"NYC Taxi Trips by Zone (Sample Data)"
        create_trips_by_zone_chart_pandas(df, title=title, save_path=args.save)


if __name__ == "__main__":
    main()
