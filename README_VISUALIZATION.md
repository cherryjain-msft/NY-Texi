# NYC Taxi Trips by City (Zone) - Multiple Line Graph Visualization

## Overview

This repository contains a **multiple line graph visualization** that represents trips by city (zone) using synthetic NYC taxi data. The visualization is implemented in the Databricks notebook `nyc_taxi_data_generation.ipynb`.

## Visualization Preview

![Trips by City Over Time](https://github.com/user-attachments/assets/0f6ecc51-cb21-4134-a686-79b128e80da5)

## What It Shows

The multiple line graph displays:
- **7 different zones/cities**: Manhattan, Brooklyn, Queens, Bronx, Staten Island, JFK Airport, LGA Airport
- **Time series data**: Monthly trip counts from 2021 to 2026
- **Comparative trends**: Easy visual comparison of trip volumes across different zones
- **Temporal patterns**: Seasonality, growth trends, and variations over time

## Key Features

### Visual Design
- ✅ **Multiple colored lines**: Each zone has a distinct color
- ✅ **Data point markers**: Circular markers at each monthly data point
- ✅ **Clear legend**: Zone names displayed outside the plot area
- ✅ **Grid lines**: Semi-transparent grid for easier value reading
- ✅ **Professional formatting**: Bold title, labeled axes, optimized size (12x6 inches)

### Data Characteristics
The visualization uses synthetic taxi data with realistic patterns:
- **Yearly trends**: Recovery from 2021 through strong growth in 2024-2025
- **Seasonality**: Higher volumes in summer, lower in winter
- **Day-of-week effects**: Weekend and holiday impacts
- **Zone-specific patterns**: Manhattan shows highest volumes, Staten Island lowest
- **Airport dynamics**: JFK and LGA show different patterns than residential areas

## Files in This Repository

### 1. `nyc_taxi_data_generation.ipynb` (Main Notebook)
The Databricks notebook containing:
- Synthetic data generation code
- Data transformation pipeline (Bronze → Silver → Gold)
- **Multiple line graph visualization** (lines 687-698)
- Additional visualizations (revenue trends, day-of-week analysis, zone heatmap)

### 2. `VISUALIZATION_GUIDE.md` (Documentation)
Comprehensive guide explaining:
- The visualization code and logic
- Data flow through the pipeline
- Features and formatting details
- How to run in Databricks

### 3. `visualization_example.py` (Standalone Example)
A Python script that demonstrates the visualization logic:
- Can run independently (doesn't require Databricks)
- Generates sample data
- Creates the same type of graph
- Useful for understanding the concept

## How to Use

### Option 1: Run in Databricks (Recommended)
1. Upload `nyc_taxi_data_generation.ipynb` to your Databricks workspace
2. Attach to a cluster with Spark runtime
3. Run all cells sequentially
4. The multiple line graph appears in the final cell output

### Option 2: Run Standalone Example (For Testing)
```bash
# Install dependencies
pip install pandas matplotlib seaborn numpy

# Run the example
python visualization_example.py
```

This generates a sample visualization saved to `/tmp/trips_by_zone.png`

## Code Snippet

The core visualization code from the notebook:

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

## Insights from the Visualization

By examining the multiple line graph, you can observe:

1. **Manhattan Dominance**: Manhattan (purple line) consistently shows the highest trip volumes
2. **Growth Trends**: All zones show overall growth from 2021 to 2025
3. **Seasonal Patterns**: Peaks in summer months, dips in winter
4. **Zone Hierarchy**: Clear volume tiers (Manhattan > Brooklyn > Queens > Bronx > Airports > Staten Island)
5. **Recovery Pattern**: 2021 shows lower volumes (simulating post-pandemic recovery)
6. **Airport Stability**: JFK and LGA show more stable patterns compared to residential zones

## Dependencies

- **For Databricks Notebook**:
  - PySpark (included in Databricks runtime)
  - matplotlib
  - seaborn
  - pandas
  - numpy

- **For Standalone Example**:
  - pandas
  - matplotlib
  - seaborn
  - numpy

## Technical Details

- **Data Aggregation**: Monthly aggregation by pickup zone
- **Time Range**: 5 years (2021-2026) with 60 monthly data points
- **Zones**: 7 NYC zones/airports
- **Format**: PNG image output at 300 DPI
- **Size**: 12 inches wide × 6 inches tall

## Next Steps

To enhance this visualization, consider:
- Adding interactive tooltips (using Plotly)
- Implementing drill-down by zone
- Adding forecast lines for future trends
- Creating animated time-series visualization
- Comparing with real NYC taxi data

## License

This is part of the NY-Texi project demonstrating data visualization techniques for taxi trip analysis.

## Support

For questions or issues:
1. Review the `VISUALIZATION_GUIDE.md` for detailed documentation
2. Check the code comments in `nyc_taxi_data_generation.ipynb`
3. Run `visualization_example.py` to test the concept locally

---

**Note**: The visualization uses **synthetic data** generated to simulate realistic NYC taxi trip patterns. It is designed for demonstration and educational purposes.
