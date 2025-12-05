"""
Trips by City Visualization Module

Purpose: Create a multiple line graph representing trips by city using synthetic taxi data.
This module visualizes trip trends across different NYC zones (Manhattan, Brooklyn, Queens, 
Bronx, Staten Island, JFK, LGA) over time.
"""

import matplotlib.pyplot as plt
from matplotlib.ticker import StrMethodFormatter
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random
from typing import Optional

# Constants for demand simulation
HOLIDAY_DEMAND_MULTIPLIER = 1.35
MIN_DEMAND_MULTIPLIER = 0.9
DEMAND_VARIANCE = 0.2


def generate_sample_taxi_data(
    start_date: str = "2021-01-01",
    end_date: str = "2025-12-31",
    base_per_day: int = 1200
) -> pd.DataFrame:
    """
    Generate synthetic NYC taxi trip data for visualization.
    
    Args:
        start_date: Start date for data generation (YYYY-MM-DD format)
        end_date: End date for data generation (YYYY-MM-DD format)
        base_per_day: Base number of trips per day
        
    Returns:
        DataFrame with synthetic taxi trip data
    """
    random.seed(42)
    np.random.seed(42)
    
    zones = ["Manhattan", "Brooklyn", "Queens", "Bronx", "Staten Island", "JFK", "LGA"]
    
    # US holidays for increased demand
    us_holidays = [
        "2021-01-01", "2021-01-18", "2021-02-15", "2021-05-31", "2021-07-04", "2021-07-05",
        "2021-09-06", "2021-10-11", "2021-11-11", "2021-11-25", "2021-12-24", "2021-12-25", "2021-12-31",
        "2022-01-01", "2022-01-17", "2022-02-21", "2022-05-30", "2022-07-04", 
        "2022-09-05", "2022-10-10", "2022-11-11", "2022-11-24", "2022-12-24", "2022-12-25", "2022-12-26", "2022-12-31",
        "2023-01-01", "2023-01-02", "2023-01-16", "2023-02-20", "2023-05-29", "2023-07-04", 
        "2023-09-04", "2023-10-09", "2023-11-10", "2023-11-23", "2023-12-24", "2023-12-25", "2023-12-31",
        "2024-01-01", "2024-01-15", "2024-02-19", "2024-05-27", "2024-07-04", 
        "2024-09-02", "2024-10-14", "2024-11-11", "2024-11-28", "2024-12-24", "2024-12-25", "2024-12-31",
        "2025-01-01", "2025-01-20", "2025-02-17", "2025-05-26", "2025-07-04", 
        "2025-09-01", "2025-10-13", "2025-11-11", "2025-11-27", "2025-12-24", "2025-12-25", "2025-12-31",
    ]
    holiday_set = {datetime.strptime(d, '%Y-%m-%d').date() for d in us_holidays}
    
    # Base demand by year
    year_demand_index = {
        2021: 0.65,  # depressed demand
        2022: 0.85,
        2023: 1.00,
        2024: 1.15,
        2025: 1.25,
    }
    
    # Month seasonality
    month_seasonality = {
        1: 0.85, 2: 0.88, 3: 0.95, 4: 1.00, 5: 1.05, 6: 1.10,
        7: 1.15, 8: 1.12, 9: 1.03, 10: 1.00, 11: 1.02, 12: 1.12
    }
    
    # Day-of-week multiplier
    dow_multiplier = {0: 1.00, 1: 1.00, 2: 1.02, 3: 1.02, 4: 1.10, 5: 1.25, 6: 1.20}
    
    # Zone pickup share
    zone_base_share = {
        "Manhattan": 0.42, "Brooklyn": 0.18, "Queens": 0.16, 
        "Bronx": 0.10, "Staten Island": 0.02, "JFK": 0.07, "LGA": 0.05
    }
    
    def generate_day_rows(day_dt, base_count=5000):
        y = day_dt.year
        m = day_dt.month
        dow = day_dt.weekday()
        is_holiday = day_dt.date() in holiday_set
        
        demand = year_demand_index.get(y, 1.0) * month_seasonality[m] * dow_multiplier[dow]
        if is_holiday:
            demand *= HOLIDAY_DEMAND_MULTIPLIER
        
        day_trips = int(base_count * demand * (MIN_DEMAND_MULTIPLIER + DEMAND_VARIANCE * np.random.rand()))
        rows = []
        
        # Normalize zone shares
        shares = np.array([zone_base_share[z] for z in zones])
        shares = shares / shares.sum()
        zone_choices = np.random.choice(zones, size=day_trips, p=shares)
        
        for i in range(day_trips):
            pickup_zone = zone_choices[i]
            rows.append({
                "trip_id": f"{y}{m:02d}{day_dt.day:02d}-{i}",
                "pickup_datetime": day_dt,
                "pickup_zone": pickup_zone,
            })
        return rows
    
    start = datetime.strptime(start_date, '%Y-%m-%d')
    end = datetime.strptime(end_date, '%Y-%m-%d')
    cur = start
    all_rows = []
    
    while cur <= end:
        all_rows.extend(generate_day_rows(cur, base_count=base_per_day))
        cur += timedelta(days=1)
    
    return pd.DataFrame(all_rows)


def aggregate_trips_by_city_monthly(df: pd.DataFrame) -> pd.DataFrame:
    """
    Aggregate trips by city (zone) and month.
    
    Args:
        df: DataFrame with taxi trip data containing 'pickup_datetime' and 'pickup_zone' columns
        
    Returns:
        DataFrame with monthly trip counts by zone
    """
    df = df.copy()
    df['pickup_datetime'] = pd.to_datetime(df['pickup_datetime'])
    df['year'] = df['pickup_datetime'].dt.year
    df['month'] = df['pickup_datetime'].dt.month
    
    # Aggregate by year, month, and zone
    monthly_data = df.groupby(['year', 'month', 'pickup_zone']).size().reset_index(name='trips')
    monthly_data['year_month'] = pd.to_datetime(
        monthly_data[['year', 'month']].assign(day=1)
    )
    
    return monthly_data


def plot_trips_by_city(
    df_monthly: pd.DataFrame,
    title: str = "Trips by City (Zone) Over Time",
    figsize: tuple = (14, 8),
    save_path: Optional[str] = None
) -> None:
    """
    Create a multiple line graph showing trips by city over time.
    
    Args:
        df_monthly: DataFrame with columns 'year_month', 'pickup_zone', and 'trips'
        title: Chart title
        figsize: Figure size as (width, height)
        save_path: Optional path to save the figure
    """
    # Define colors for each zone
    zone_colors = {
        "Manhattan": "#1f77b4",      # Blue
        "Brooklyn": "#ff7f0e",       # Orange
        "Queens": "#2ca02c",         # Green
        "Bronx": "#d62728",          # Red
        "Staten Island": "#9467bd",  # Purple
        "JFK": "#8c564b",            # Brown
        "LGA": "#e377c2"             # Pink
    }
    
    # Create the figure
    fig, ax = plt.subplots(figsize=figsize)
    
    # Plot each zone as a separate line
    for zone in df_monthly['pickup_zone'].unique():
        zone_data = df_monthly[df_monthly['pickup_zone'] == zone].sort_values('year_month')
        color = zone_colors.get(zone, '#333333')
        ax.plot(
            zone_data['year_month'], 
            zone_data['trips'], 
            marker='o', 
            label=zone, 
            linewidth=2, 
            markersize=4,
            color=color,
            alpha=0.8
        )
    
    # Customize the chart
    ax.set_title(title, fontsize=16, fontweight='bold', pad=20)
    ax.set_xlabel('Month', fontsize=12, fontweight='bold')
    ax.set_ylabel('Number of Trips', fontsize=12, fontweight='bold')
    
    # Format y-axis with commas for thousands
    ax.yaxis.set_major_formatter(StrMethodFormatter('{x:,.0f}'))
    
    # Add legend outside the plot
    ax.legend(
        title='Zone', 
        bbox_to_anchor=(1.02, 1), 
        loc='upper left',
        fontsize=10,
        title_fontsize=11
    )
    
    # Add grid for better readability
    ax.grid(True, alpha=0.3, linestyle='--')
    
    # Rotate x-axis labels for better readability
    plt.xticks(rotation=45, ha='right')
    
    # Adjust layout to prevent legend cutoff
    plt.tight_layout()
    
    # Save if path provided
    if save_path:
        plt.savefig(save_path, dpi=150, bbox_inches='tight')
        print(f"Figure saved to: {save_path}")
    
    plt.show()


def create_trips_by_city_visualization(
    df: Optional[pd.DataFrame] = None,
    save_path: Optional[str] = None
) -> pd.DataFrame:
    """
    Main function to create the trips by city visualization.
    
    Args:
        df: Optional pre-existing DataFrame with taxi data. 
            If None, synthetic data will be generated.
        save_path: Optional path to save the figure
        
    Returns:
        DataFrame with monthly aggregated data
    """
    # Generate sample data if not provided
    if df is None:
        print("Generating synthetic NYC taxi data...")
        df = generate_sample_taxi_data()
        print(f"Generated {len(df):,} trip records")
    
    # Aggregate by month and zone
    print("Aggregating trips by city and month...")
    df_monthly = aggregate_trips_by_city_monthly(df)
    
    # Create visualization
    print("Creating multiple line graph...")
    plot_trips_by_city(df_monthly, save_path=save_path)
    
    return df_monthly


if __name__ == "__main__":
    # Run the visualization
    df_monthly = create_trips_by_city_visualization(
        save_path="trips_by_city.png"
    )
    
    # Print summary statistics
    print("\n" + "=" * 60)
    print("SUMMARY STATISTICS")
    print("=" * 60)
    
    total_by_zone = df_monthly.groupby('pickup_zone')['trips'].sum().sort_values(ascending=False)
    print("\nTotal trips by zone:")
    for zone, trips in total_by_zone.items():
        print(f"  {zone}: {trips:,}")
    
    print(f"\nTotal trips: {total_by_zone.sum():,}")
    print(f"Date range: {df_monthly['year_month'].min()} to {df_monthly['year_month'].max()}")
    print("=" * 60)
