"""
Trips by City Multi-Line Graph Generator

This script creates a multiple line graph representing trips by city (zone) 
using the synthetic NYC taxi data. Each line represents a different city/zone,
allowing for comparison of trip volumes across geographic areas over time.

Usage in Databricks:
    %run "./scripts/trips_by_city_graph"
    
Usage locally (with PySpark):
    python scripts/trips_by_city_graph.py
"""

import matplotlib.pyplot as plt
import pandas as pd
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def plot_trips_by_city(
    data: pd.DataFrame,
    year_column: str = "year",
    month_column: str = "month",
    city_column: str = "pickup_zone",
    trips_column: str = "trips",
    title: str = "Trips by City (Zone) Over Time",
    figsize: tuple = (12, 6),
    save_path: str = None
) -> None:
    """
    Create a multiple line graph showing trips by city (zone) over time.
    
    Each city/zone is represented by a separate line, allowing comparison
    of trip volumes across different geographic areas over time.
    
    Args:
        data: DataFrame containing the trip data with year, month, city, and trips columns
        year_column: Column name for year
        month_column: Column name for month
        city_column: Column name for city/zone identifier
        trips_column: Column name for trip count metric
        title: Title for the chart
        figsize: Figure size as (width, height) tuple
        save_path: Optional path to save the figure (if None, displays the plot)
    """
    logger.info("Creating trips by city multi-line chart")
    
    if data.empty:
        logger.warning("No data provided")
        return
    
    # Create a copy to avoid modifying the original data
    pdf = data.copy()
    
    # Create a date column for the x-axis
    pdf['year_month'] = pd.to_datetime(
        pdf[[year_column, month_column]].assign(day=1)
    )
    
    # Create the multi-line plot
    fig, ax = plt.subplots(figsize=figsize)
    
    # Plot a line for each city/zone
    for city, city_data in pdf.groupby(city_column):
        city_data = city_data.sort_values('year_month')
        ax.plot(
            city_data['year_month'], 
            city_data[trips_column], 
            marker='o', 
            label=city, 
            linewidth=2, 
            markersize=4
        )
    
    ax.set_xlabel('Month', fontsize=12)
    ax.set_ylabel('Number of Trips', fontsize=12)
    ax.set_title(title, fontsize=14, fontweight='bold')
    ax.legend(title='Zone', bbox_to_anchor=(1.05, 1), loc='upper left')
    ax.grid(True, alpha=0.3)
    ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: f'{int(x):,}'))
    
    plt.xticks(rotation=45)
    plt.tight_layout()
    
    if save_path:
        plt.savefig(save_path, dpi=150, bbox_inches='tight')
        logger.info(f"✓ Chart saved to {save_path}")
    else:
        plt.show()
    
    logger.info("✓ Trips by city multi-line chart created")


def create_trips_by_city_from_spark(spark, gold_table: str, catalog: str = "gm_demo", schema: str = "gm_test_schema") -> None:
    """
    Create trips by city chart from a Spark DataFrame (gold layer table).
    
    Args:
        spark: SparkSession
        gold_table: Name of the gold table containing monthly aggregations
        catalog: Catalog name
        schema: Schema name
    """
    full_table_name = f"{catalog}.{schema}.{gold_table}"
    
    logger.info(f"Loading data from {full_table_name}")
    
    df = spark.sql(f"""
        SELECT 
            year,
            month,
            pickup_zone,
            trips
        FROM {full_table_name}
        ORDER BY year, month, pickup_zone
    """)
    
    pdf = df.toPandas()
    
    plot_trips_by_city(pdf)


# Example usage with sample data for testing
if __name__ == "__main__":
    import numpy as np
    
    # Generate sample data for demonstration
    np.random.seed(42)
    
    zones = ["Manhattan", "Brooklyn", "Queens", "Bronx", "Staten Island", "JFK", "LGA"]
    years = [2023, 2024]
    months = list(range(1, 13))
    
    # Base trip counts by zone (Manhattan has the most trips)
    base_trips = {
        "Manhattan": 50000,
        "Brooklyn": 25000,
        "Queens": 20000,
        "Bronx": 12000,
        "Staten Island": 3000,
        "JFK": 8000,
        "LGA": 6000
    }
    
    data = []
    for year in years:
        for month in months:
            for zone in zones:
                # Add some seasonality and randomness
                seasonal_factor = 1 + 0.2 * np.sin(2 * np.pi * (month - 3) / 12)
                yearly_growth = 1.1 if year == 2024 else 1.0
                trips = int(base_trips[zone] * seasonal_factor * yearly_growth * (0.9 + 0.2 * np.random.random()))
                data.append({
                    "year": year,
                    "month": month,
                    "pickup_zone": zone,
                    "trips": trips
                })
    
    sample_df = pd.DataFrame(data)
    
    print("Sample data preview:")
    print(sample_df.head(20))
    print(f"\nTotal records: {len(sample_df)}")
    
    # Create the visualization
    plot_trips_by_city(sample_df)
