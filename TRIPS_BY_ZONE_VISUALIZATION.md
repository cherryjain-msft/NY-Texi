# Trips by Zone Visualization

## Overview

This feature provides a **multiple line graph** visualization showing taxi trip trends across different NYC zones/boroughs over time. Each zone is represented as a separate colored line, making it easy to compare trip volumes and identify patterns across different areas.

## Visualization Features

- **Multiple lines**: Each zone/borough is shown as a distinct colored line
- **Time series**: Shows trip trends over time (daily, monthly, etc.)
- **Color-coded**: Uses a color palette to differentiate zones
- **Interactive legend**: Shows all zones with their corresponding colors
- **Summary statistics**: Displays total and average trips per zone
- **Professional styling**: Clean, publication-ready charts

## Zones Covered

The visualization tracks trips across NYC zones:
- **Manhattan**: High-density urban core
- **Brooklyn**: Major borough with diverse neighborhoods
- **Queens**: Residential and commercial areas
- **Bronx**: Northern borough
- **Staten Island**: Island borough
- **JFK**: JFK Airport
- **LGA**: LaGuardia Airport

## Usage

### Option 1: Standalone Script (Recommended for Quick Start)

The standalone script can work with or without a Databricks environment:

```bash
# Generate visualization with sample data (no Databricks needed)
python create_trips_by_zone_visualization.py --use-sample

# Save the chart to a file
python create_trips_by_zone_visualization.py --use-sample --save trips_by_zone.png

# Customize date range for sample data
python create_trips_by_zone_visualization.py --use-sample \
    --start-date 2023-01-01 \
    --end-date 2023-12-31

# Use real Databricks data
python create_trips_by_zone_visualization.py --use-databricks --year 2023

# Use custom catalog and schema
python create_trips_by_zone_visualization.py --use-databricks \
    --catalog my_catalog \
    --schema my_schema \
    --year 2024
```

### Option 2: Using the Analytics Module

From Python code or Databricks notebooks:

```python
from pyspark.sql import SparkSession
from src.gm_analysis.analytics.visualize_metrics import create_trips_by_zone_chart

# Initialize Spark
spark = SparkSession.builder.getOrCreate()

# Create the visualization
create_trips_by_zone_chart(
    spark=spark,
    catalog="gm_demo",
    schema="gm_test_schema",
    table_name="gold_gm_data_2023_daily",
    title="NYC Taxi Trips by Zone - 2023"
)
```

### Option 3: Using the AnalyticsVisualizer Class

For more control and integration with other visualizations:

```python
from pyspark.sql import SparkSession
from src.gm_analysis.analytics.visualize_metrics import AnalyticsVisualizer

spark = SparkSession.builder.getOrCreate()
viz = AnalyticsVisualizer(
    spark=spark,
    catalog="gm_demo",
    schema="gm_test_schema"
)

# Create trips by zone visualization
viz.plot_trips_by_zone(
    table_name="gold_gm_data_2023_daily",
    date_column="pickup_date",
    zone_column="pickup_zone",
    trips_column="trips",
    title="NYC Taxi Trips by Zone Over Time"
)
```

### Option 4: Analytics Dashboard Notebook

The visualization is also available in the analytics dashboard:

```bash
# Run the analytics dashboard in Databricks
databricks notebook run notebooks/02_analytics_dashboard.py
```

The dashboard includes a dedicated section for "Trips by Zone Over Time" that automatically detects the appropriate columns and generates the visualization.

## Data Requirements

### For Databricks/PySpark Mode

The gold layer table should contain:
- **Date column**: `pickup_date`, `agg_date`, or similar
- **Zone column**: `pickup_zone`, `zone`, `city`, or `borough`
- **Trips column**: `trips`, `trip_count`, `num_trips`, or `record_count`

Example gold layer schema:
```
pickup_date (date)
pickup_zone (string)
trips (long)
revenue (double)
avg_fare (double)
...
```

### For Sample Data Mode

No specific data requirements - the script generates synthetic data automatically.

## Installation

### Required Python Packages

```bash
pip install matplotlib seaborn pandas numpy

# For Databricks/PySpark mode (optional)
pip install pyspark
```

### Adding to Existing Project

The visualization is already integrated into:
1. `src/gm_analysis/analytics/visualize_metrics.py` - Core module
2. `notebooks/02_analytics_dashboard.py` - Dashboard notebook
3. `create_trips_by_zone_visualization.py` - Standalone script

## Output

### Visual Output

The visualization produces a professional multi-line graph showing:
- X-axis: Date/Time
- Y-axis: Number of trips (formatted with thousands separators)
- Multiple colored lines representing each zone
- Legend identifying each zone
- Grid for easy reading
- Rotated date labels for clarity

### Text Output

Summary statistics including:
- Total trips per zone
- Average daily trips per zone
- Overall total trips across all zones

Example output:
```
================================================================================
TRIPS BY ZONE SUMMARY
================================================================================
  Manhattan           : 2,099,526 total trips | 5,752 avg daily
  Brooklyn            : 1,249,756 total trips | 3,423 avg daily
  Queens              : 1,034,676 total trips | 2,834 avg daily
  Bronx               : 629,070 total trips | 1,723 avg daily
  JFK                 : 500,278 total trips | 1,370 avg daily
  LGA                 : 417,640 total trips | 1,144 avg daily
  Staten Island       : 205,512 total trips | 563 avg daily
  ────────────────────────────────────────────────────────────────────────────
  TOTAL               : 6,136,458 trips
================================================================================
```

## Use Cases

1. **Trend Analysis**: Identify seasonal patterns and growth trends by zone
2. **Comparative Analysis**: Compare trip volumes across different zones
3. **Business Intelligence**: Support decision-making with visual insights
4. **Reporting**: Generate professional charts for stakeholders
5. **Anomaly Detection**: Spot unusual patterns or outliers
6. **Capacity Planning**: Understand demand distribution across zones

## Customization

### Modify Colors

Edit the color palette in the visualization code:
```python
# Default uses Set2 color map
colors = plt.cm.Set2(range(len(zones)))

# Use different color map
colors = plt.cm.tab10(range(len(zones)))
```

### Adjust Figure Size

```python
plt.rcParams['figure.figsize'] = (20, 10)  # Larger chart
```

### Change Line Style

```python
ax.plot(
    ...,
    linestyle='--',  # Dashed lines
    linewidth=3,     # Thicker lines
    marker='s'       # Square markers
)
```

## Troubleshooting

### Issue: "No module named matplotlib"
**Solution**: Install required packages:
```bash
pip install matplotlib seaborn pandas numpy
```

### Issue: "Table not found"
**Solution**: Ensure the gold layer table exists and you have access:
```python
# Check if table exists
spark.sql("SHOW TABLES IN gm_demo.gm_test_schema").show()
```

### Issue: "Required columns not found"
**Solution**: The script auto-detects columns. Check your table schema:
```python
spark.table("gm_demo.gm_test_schema.gold_gm_data_2023_daily").printSchema()
```

### Issue: Empty or no visualization
**Solution**: Verify data exists in the table:
```python
spark.table("gm_demo.gm_test_schema.gold_gm_data_2023_daily").count()
```

## Examples

See the generated example visualization showing NYC taxi trips by zone with:
- Clear differentiation between zones
- Seasonal patterns (summer peak, winter trough)
- Weekend spikes in trip volume
- Comparative view across all NYC areas

## Integration with Medallion Architecture

This visualization is designed to work with the gold layer of the medallion architecture:

```
Bronze Layer (Raw Data)
    ↓
Silver Layer (Cleansed)
    ↓
Gold Layer (Aggregated) ← Trips by Zone visualization uses this
    ↓
Analytics & Dashboards
```

The gold layer aggregates trip data by date and zone, providing the perfect input for this visualization.

## Performance Considerations

- **Large Datasets**: The visualization converts Spark DataFrames to Pandas. For very large datasets (millions of rows), consider:
  - Pre-filtering by date range
  - Using monthly aggregations instead of daily
  - Sampling if appropriate for your use case

- **Optimization**: The script uses efficient PySpark operations before conversion to Pandas

## Future Enhancements

Potential improvements:
- Interactive Plotly version for web dashboards
- Filtering by date range in the UI
- Hover tooltips with detailed statistics
- Export to multiple formats (PNG, SVG, PDF)
- Comparison mode (multiple years side-by-side)
- Animated version showing progression over time

## License

Part of the NY-Texi project. See repository LICENSE for details.

## Support

For issues or questions:
1. Check this documentation
2. Review example outputs
3. Test with sample data first
4. Verify gold layer data exists and is accessible
