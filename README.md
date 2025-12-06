# NY Taxi Data Analysis

A comprehensive data analysis project featuring NYC taxi trip data with medallion architecture (Bronze → Silver → Gold) and advanced visualizations.

## 🎯 Key Features

### 📊 **NEW: Multiple Line Graph - Trips by Zone**

Visualize taxi trip trends across different NYC zones/boroughs over time with our new multi-line graph feature!

![NYC Taxi Trips by Zone](https://github.com/user-attachments/assets/a726b634-b26d-4be9-9287-70da744ef14e)

**Features:**
- Track trips across Manhattan, Brooklyn, Queens, Bronx, Staten Island, JFK, and LGA
- Compare trip volumes and identify patterns across zones
- Seasonal trend analysis with color-coded zones
- Professional, publication-ready visualizations
- Works with real data or generates synthetic sample data

**Quick Start:**
```bash
# Generate visualization with sample data (no setup required!)
python create_trips_by_zone_visualization.py --use-sample --save trips_by_zone.png
```

See [TRIPS_BY_ZONE_VISUALIZATION.md](TRIPS_BY_ZONE_VISUALIZATION.md) for detailed documentation.

### 🏗️ Medallion Architecture

Complete data pipeline implementation:
- **Bronze Layer**: Raw data ingestion with audit metadata
- **Silver Layer**: Data cleansing and standardization
- **Gold Layer**: Business-ready aggregations and metrics
- **Unity Catalog**: Governance and access control
- **Delta Lake**: ACID transactions and time travel

### 📈 Analytics & Visualizations

Comprehensive analytics suite including:
- **Trips by Zone Over Time** (NEW!) - Multi-line graph showing trip trends
- Data volume trends
- Data quality metrics
- Layer comparison charts
- Business metrics dashboards
- Performance analytics

### 🎲 Synthetic Data Generation

Generate realistic NYC taxi trip data with:
- Yearly trends (2021-2025)
- Seasonal patterns (summer peaks, winter troughs)
- Weekend and holiday variations
- Zone-specific patterns
- Realistic fare calculations

## 🚀 Quick Start

### Option 1: Visualize Sample Data (No Dependencies)

```bash
# Install dependencies
pip install matplotlib seaborn pandas numpy

# Generate and visualize sample data
python create_trips_by_zone_visualization.py --use-sample
```

### Option 2: Run Full Pipeline with Databricks

```bash
# 1. Data discovery
databricks notebook run notebooks/01_data_discovery.py --parameters year=2023

# 2. Run the medallion pipeline
python run_pipeline.py --year 2023

# 3. View analytics dashboard
databricks notebook run notebooks/02_analytics_dashboard.py
```

### Option 3: Generate Synthetic Data

Open and run the Jupyter notebook:
```bash
jupyter notebook nyc_taxi_data_generation.ipynb
```

## 📁 Project Structure

```
NY-Texi/
├── create_trips_by_zone_visualization.py  # NEW: Standalone visualization script
├── TRIPS_BY_ZONE_VISUALIZATION.md         # NEW: Visualization documentation
├── nyc_taxi_data_generation.ipynb         # Synthetic data generation
├── notebooks/
│   ├── 01_data_discovery.py               # Data exploration
│   └── 02_analytics_dashboard.py          # Analytics dashboard (with trips by zone)
├── src/
│   └── gm_analysis/
│       ├── analytics/
│       │   └── visualize_metrics.py       # Visualization module (updated)
│       ├── bronze/
│       │   └── ingest_data.py             # Bronze layer ingestion
│       ├── silver/
│       │   └── cleanse_data.py            # Silver layer transformation
│       ├── gold/
│       │   └── aggregate_metrics.py       # Gold layer aggregation
│       └── sql/
│           ├── optimize.py                # Delta optimization
│           └── validate_delta.py          # Data quality validation
├── config/                                 # Configuration files
├── resources/                              # Databricks resources
└── run_pipeline.py                        # Main pipeline orchestrator
```

## 🎨 Visualization Examples

### Trips by Zone Over Time
The new multi-line graph shows trip patterns across all NYC zones, with:
- Manhattan showing highest volume (5,752 avg daily trips)
- Clear seasonal patterns with summer peaks
- Weekend spikes across all zones
- Airport (JFK/LGA) showing distinct patterns

### Analytics Dashboard
Comprehensive metrics including:
- Data volume trends
- Data quality scores
- Layer comparison charts
- Monthly aggregations
- Performance analytics

## 🔧 Installation

### Basic (Visualization Only)
```bash
pip install matplotlib seaborn pandas numpy
```

### Full Stack (with PySpark/Databricks)
```bash
pip install matplotlib seaborn pandas numpy pyspark
```

## 📚 Documentation

- **[Visualization Guide](TRIPS_BY_ZONE_VISUALIZATION.md)** - Detailed guide for the trips by zone visualization
- **[GM Analysis Guide](README_GM_ANALYSIS.md)** - Complete medallion architecture implementation
- **[Quick Start](QUICKSTART.md)** - Quick start guide
- **[Project Summary](PROJECT_SUMMARY.md)** - Implementation summary

## 🎯 Use Cases

1. **Data Analysis**: Explore NYC taxi trip patterns and trends
2. **Visualization**: Create professional charts and dashboards  
3. **Learning**: Understand medallion architecture and Delta Lake
4. **Prototyping**: Generate synthetic data for testing
5. **Reporting**: Produce stakeholder-ready visualizations

## 🌟 Highlights

### What Makes This Special?

✅ **Production-Ready**: Follows Databricks best practices  
✅ **Comprehensive**: End-to-end data pipeline  
✅ **Visual**: Professional visualizations included  
✅ **Flexible**: Works with real or synthetic data  
✅ **Documented**: Extensive documentation and examples  
✅ **Tested**: Validated with sample data  

### New Visualization Features

✅ **Multi-line graphs**: Compare multiple zones simultaneously  
✅ **Auto-detection**: Automatically finds appropriate columns  
✅ **Sample data mode**: Test without Databricks setup  
✅ **Customizable**: Easy to adapt to your needs  
✅ **Professional output**: Publication-quality charts  

## 📊 Sample Output

When running the trips by zone visualization, you'll get:

**Visual Output:**
- Multi-line graph with color-coded zones
- Time series from start to end date
- Legend showing all zones
- Formatted axis labels and grid

**Summary Statistics:**
```
TRIPS BY ZONE SUMMARY
Manhattan           : 2,099,526 total trips | 5,752 avg daily
Brooklyn            : 1,249,756 total trips | 3,423 avg daily
Queens              : 1,034,676 total trips | 2,834 avg daily
Bronx               : 629,070 total trips | 1,723 avg daily
JFK                 : 500,278 total trips | 1,370 avg daily
LGA                 : 417,640 total trips | 1,144 avg daily
Staten Island       : 205,512 total trips | 563 avg daily
TOTAL               : 6,136,458 trips
```

## 🤝 Contributing

Contributions welcome! Areas for enhancement:
- Additional visualization types
- Interactive dashboards (Plotly/Dash)
- Real-time data streaming
- Machine learning models
- More synthetic data scenarios

## 📝 License

This project is part of the NY-Texi analysis framework.

## 🆘 Support

For questions or issues:
1. Check the documentation files
2. Review example outputs and screenshots
3. Test with sample data first
4. Verify your environment setup

## 🎓 Learning Resources

- **Medallion Architecture**: See `README_GM_ANALYSIS.md`
- **Delta Lake Best Practices**: Implemented throughout
- **Data Visualization**: Check `TRIPS_BY_ZONE_VISUALIZATION.md`
- **Synthetic Data**: Explore `nyc_taxi_data_generation.ipynb`

---

**Ready to explore NYC taxi data? Start with the trips by zone visualization!**

```bash
python create_trips_by_zone_visualization.py --use-sample
```
