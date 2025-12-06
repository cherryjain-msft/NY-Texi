"""
NYC Taxi Data Visualization Example
Multiple Line Graph: Trips by City (Zone) Over Time

This script demonstrates the visualization logic used in the Databricks notebook.
Note: This is a reference implementation. The actual visualization runs in Databricks
with Spark-generated data.
"""

import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
from datetime import datetime, timedelta


def generate_sample_data():
    """
    Generate sample monthly trip data by zone for demonstration.
    In the actual notebook, this data comes from Spark aggregations.
    """
    zones = ['Manhattan', 'Brooklyn', 'Queens', 'Bronx', 'Staten Island', 'JFK', 'LGA']
    start_date = datetime(2021, 1, 1)
    months = pd.date_range(start=start_date, periods=60, freq='MS')  # 5 years monthly
    
    data = []
    for zone in zones:
        # Base trip volume varies by zone
        base_trips = {
            'Manhattan': 50000,
            'Brooklyn': 30000,
            'Queens': 25000,
            'Bronx': 15000,
            'Staten Island': 5000,
            'JFK': 20000,
            'LGA': 18000
        }[zone]
        
        for i, month in enumerate(months):
            # Add growth trend
            growth_factor = 1 + (i / 60) * 0.5  # 50% growth over 5 years
            
            # Add seasonality (summer higher)
            seasonal_factor = 1 + 0.2 * np.sin(2 * np.pi * i / 12)
            
            # Add some randomness
            random_factor = np.random.uniform(0.9, 1.1)
            
            trips = int(base_trips * growth_factor * seasonal_factor * random_factor)
            
            data.append({
                'year': month.year,
                'month': month.month,
                'pickup_zone': zone,
                'trips': trips,
                'year_month': month
            })
    
    return pd.DataFrame(data)


def create_multiple_line_graph(pdf_monthly, save_path=None):
    """
    Create a multiple line graph showing trips by city (zone) over time.
    
    This function replicates the visualization from the Databricks notebook.
    
    Args:
        pdf_monthly: Pandas DataFrame with columns: year_month, pickup_zone, trips
        save_path: Optional path to save the figure
    """
    # Create the figure
    plt.figure(figsize=(12, 6))
    
    # Plot a line for each zone
    for zone, zone_data in pdf_monthly.groupby('pickup_zone'):
        # Sort by date to ensure proper line plotting
        zone_data = zone_data.sort_values('year_month')
        
        # Plot the line
        plt.plot(zone_data['year_month'], 
                zone_data['trips'], 
                marker='o',          # Circle markers at data points
                label=zone,          # Legend label
                linewidth=2,         # Line thickness
                markersize=4)        # Marker size
    
    # Formatting
    plt.title('Trips by City (Zone) Over Time', fontsize=14, fontweight='bold')
    plt.xlabel('Month', fontsize=12)
    plt.ylabel('Number of Trips', fontsize=12)
    
    # Position legend outside plot area to avoid overlap
    plt.legend(title='Zone', bbox_to_anchor=(1.05, 1), loc='upper left')
    
    # Add grid for better readability
    plt.grid(True, alpha=0.3)
    
    # Adjust layout to prevent label cutoff
    plt.tight_layout()
    
    # Save if path provided
    if save_path:
        plt.savefig(save_path, dpi=300, bbox_inches='tight')
        print(f"Figure saved to: {save_path}")
    
    # Display the plot
    plt.show()
    
    return plt.gcf()


def main():
    """
    Main function to demonstrate the visualization.
    """
    print("Generating sample NYC taxi trip data...")
    pdf_monthly = generate_sample_data()
    
    print(f"\nData shape: {pdf_monthly.shape}")
    print(f"Date range: {pdf_monthly['year_month'].min()} to {pdf_monthly['year_month'].max()}")
    print(f"Zones: {pdf_monthly['pickup_zone'].unique().tolist()}")
    
    print("\nSample data:")
    print(pdf_monthly.head(10))
    
    print("\nCreating multiple line graph visualization...")
    create_multiple_line_graph(pdf_monthly, save_path='/tmp/trips_by_zone.png')
    
    print("\nVisualization complete!")
    print("\nThis demonstrates the same logic used in the Databricks notebook")
    print("at lines 687-698 of nyc_taxi_data_generation.ipynb")


if __name__ == "__main__":
    main()
