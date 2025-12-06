# NYC Taxi Data Visualization Guide

## Multiple Line Graph: Trips by City (Zone) Over Time

### Overview
The notebook `nyc_taxi_data_generation.ipynb` includes a comprehensive visualization showing trips by city (zone) over time using synthetic taxi data.

### Location in Notebook
The multiple line graph code is located in the final code cell of the notebook (lines 687-698).

### Code Description

```python
# Multiple line graph: Trips by City (Zone) over time
plt.figure(figsize=(12, 6))
for zone, zone_data in pdf_monthly.groupby('pickup_zone'):
    zone_data = zone_data.sort_values('year_month')
    plt.plot(zone_data['year_month'], zone_data['trips'], 
             marker='o', label=zone, linewidth=2, markersize=4)
plt.title('Trips by City (Zone) Over Time', fontsize=14, fontweight='bold')
plt.xlabel('Month', fontsize=12)
plt.ylabel('Number of Trips', fontsize=12)
plt.legend(title='Zone', bbox_to_anchor=(1.05, 1), loc='upper left')
plt.grid(True, alpha=0.3)
plt.tight_layout()
plt.show()
```

### Features

1. **Data Source**: Uses `pdf_monthly` DataFrame which contains aggregated monthly trip data by zone
2. **Multiple Lines**: Each line represents a different zone (city):
   - Manhattan
   - Brooklyn
   - Queens
   - Bronx
   - Staten Island
   - JFK (airport)
   - LGA (airport)

3. **Visual Elements**:
   - **Figure Size**: 12x6 inches for optimal readability
   - **Line Style**: Solid lines with circular markers
   - **Line Properties**: Width=2, Marker size=4
   - **Grid**: Semi-transparent (alpha=0.3) for better readability
   - **Legend**: Positioned outside the plot area to avoid overlap

4. **Axes**:
   - **X-axis**: Time (Month) in chronological order
   - **Y-axis**: Number of trips per zone

5. **Title**: "Trips by City (Zone) Over Time" - bold, 14pt font

### Data Flow

1. **Bronze Layer**: Raw synthetic taxi trip data generated with realistic patterns
2. **Silver Layer**: Cleaned and transformed data (`df_silver`)
3. **Gold Layer**: Monthly aggregations by zone (`df_gold_monthly`)
4. **Pandas Conversion**: `pdf_monthly` - converted to Pandas for plotting
5. **Visualization**: Multiple line graph showing trends by zone

### Expected Output

The graph displays:
- Temporal trends showing how trip volumes change over time for each zone
- Comparative view across different zones
- Seasonal patterns and year-over-year growth
- Weekend and holiday impacts on trip volumes by location

### Dependencies

- matplotlib
- seaborn
- pandas
- PySpark (for data generation and aggregation)

### Running the Visualization

This notebook is designed to run in Databricks environment:

1. Upload the notebook to Databricks workspace
2. Attach to a cluster with Spark runtime
3. Run all cells sequentially
4. The visualization appears in the output of the final cell

### Notes

- The synthetic data includes realistic patterns:
  - Yearly trends (2021-2025)
  - Seasonal variations
  - Day-of-week patterns
  - Holiday impacts
  - Time-of-day variations
  - Zone-specific characteristics

- The multiple line graph effectively shows how different zones (cities) compare in trip volume over time
- Each zone has distinct patterns based on its characteristics (e.g., airports show different patterns than residential areas)
