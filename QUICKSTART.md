# GM Data Analysis - Quick Start Guide

## 🚀 Quick Setup (5 Minutes)

### 1. Run Data Discovery
```bash
# Execute the data discovery notebook
databricks notebook run notebooks/01_data_discovery.py --parameters year=2023
```

**What it does:**
- Explores `/Volumes/gm_demo/gm_test_schema/gm_volume/2023/`
- Identifies file formats and schemas
- Analyzes data quality
- Recommends partitioning strategies

### 2. Update Configuration

After data discovery, update these files with your actual column names:

**File: `run_pipeline.py`** - Update lines 75-80:
```python
key_columns=["your_id_column"],           # Replace with actual key
string_columns=["name", "category"],       # Your string columns
partition_cols=["your_date_column"]        # Your partition column
```

**File: `run_pipeline.py`** - Update lines 105-109:
```python
date_column="your_date_column",            # Your date/timestamp column
metric_columns=["amount", "count"],        # Your numeric columns
dimension_columns=["category", "region"]   # Your categorical columns
```

### 3. Run the Pipeline

```bash
# Full pipeline execution
python run_pipeline.py --year 2023

# Or run individual steps
python run_pipeline.py --year 2023 --step bronze
python run_pipeline.py --year 2023 --step silver
python run_pipeline.py --year 2023 --step gold
```

### 4. View Results

```bash
# Run the analytics dashboard
databricks notebook run notebooks/02_analytics_dashboard.py
```

## 📋 Common Commands

### Data Ingestion
```python
# Bronze layer only
python run_pipeline.py --year 2023 --step bronze

# Skip bronze (if already loaded)
python run_pipeline.py --year 2023 --skip-bronze
```

### Data Quality
```python
# Run validation only
python run_pipeline.py --year 2023 --step validation

# Skip validation
python run_pipeline.py --year 2023 --skip-validation
```

### Optimization
```python
# Optimize without VACUUM
python run_pipeline.py --year 2023 --step optimization

# Optimize with VACUUM (removes old files)
python run_pipeline.py --year 2023 --step optimization --vacuum
```

### Full Pipeline
```python
# Complete pipeline with VACUUM
python run_pipeline.py --year 2023 --vacuum

# Complete pipeline, skip bronze
python run_pipeline.py --year 2023 --skip-bronze --vacuum
```

## 🔍 Verification Queries

### Check Table Counts
```sql
-- Bronze layer
SELECT COUNT(*) FROM gm_demo.gm_test_schema.bronze_gm_data_2023;

-- Silver layer
SELECT COUNT(*) FROM gm_demo.gm_test_schema.silver_gm_data_2023;

-- Gold layers
SELECT COUNT(*) FROM gm_demo.gm_test_schema.gold_gm_data_2023_daily;
SELECT COUNT(*) FROM gm_demo.gm_test_schema.gold_gm_data_2023_monthly;
```

### View Data Quality
```sql
-- Latest validation results
SELECT * 
FROM gm_demo.gm_test_schema.data_quality_validations
ORDER BY timestamp DESC
LIMIT 10;

-- Completeness scores
SELECT 
  AVG(_completeness_score) as avg_completeness,
  MIN(_completeness_score) as min_completeness,
  MAX(_completeness_score) as max_completeness
FROM gm_demo.gm_test_schema.silver_gm_data_2023;
```

### Check Table Details
```sql
-- Table metadata
DESCRIBE DETAIL gm_demo.gm_test_schema.silver_gm_data_2023;

-- Table history
DESCRIBE HISTORY gm_demo.gm_test_schema.silver_gm_data_2023 LIMIT 10;

-- Table properties
SHOW TBLPROPERTIES gm_demo.gm_test_schema.silver_gm_data_2023;
```

## 🔧 Troubleshooting

### Issue: "Table not found"
```bash
# Verify catalog and schema exist
databricks sql -e "SHOW CATALOGS;"
databricks sql -e "SHOW SCHEMAS IN gm_demo;"
databricks sql -e "SHOW TABLES IN gm_demo.gm_test_schema;"
```

### Issue: "Permission denied"
```bash
# Run grants to set up permissions
databricks sql -f src/gm_analysis/sql/grants.sql
```

### Issue: "Column not found"
- Run data discovery first: `notebooks/01_data_discovery.py`
- Update column names in `run_pipeline.py`
- Re-run the pipeline

### Issue: "Out of memory"
- Increase cluster size in `databricks.yml`
- Enable auto-scaling
- Partition data more granularly

## 📊 Daily Operations

### Morning Checks
```bash
# 1. Check pipeline status
databricks jobs list-runs --job-id <job-id> --limit 5

# 2. View validation results
databricks sql -e "SELECT * FROM gm_demo.gm_test_schema.data_quality_validations ORDER BY timestamp DESC LIMIT 5;"

# 3. Check table counts
python -c "
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
print('Bronze:', spark.table('gm_demo.gm_test_schema.bronze_gm_data_2023').count())
print('Silver:', spark.table('gm_demo.gm_test_schema.silver_gm_data_2023').count())
"
```

### Weekly Maintenance
```bash
# Run optimization with VACUUM
python run_pipeline.py --year 2023 --step optimization --vacuum

# Or use the maintenance workflow
databricks bundle run gm_maintenance_job
```

## 📁 File Structure Reference

```
├── run_pipeline.py                    # Main orchestration script ⭐
├── notebooks/
│   ├── 01_data_discovery.py          # Data exploration
│   └── 02_analytics_dashboard.py     # Analytics dashboard
├── src/gm_analysis/
│   ├── bronze/ingest_data.py         # Bronze ingestion
│   ├── silver/cleanse_data.py        # Silver transformation
│   ├── gold/aggregate_metrics.py     # Gold aggregation
│   ├── analytics/visualize_metrics.py # Visualizations
│   └── sql/
│       ├── optimize.py               # Table optimization
│       ├── validate_delta.py         # Data quality
│       └── grants.sql                # Access control
└── resources/
    ├── workflows.yml                  # Job definitions
    └── unity_catalog.yml             # Catalog config
```

## 🎯 Next Steps

1. ✅ Run data discovery
2. ✅ Update column configurations
3. ✅ Execute pipeline
4. ✅ Verify data quality
5. ✅ Set up workflows
6. ✅ Grant access to stakeholders
7. ✅ Monitor and maintain

## 📞 Support

- **Data Engineering**: data-engineering@company.com
- **Documentation**: README_GM_ANALYSIS.md
- **Logs**: `pipeline_YYYYMMDD_HHMMSS.log`

---

**Remember**: Always run data discovery first to understand your data structure before executing the full pipeline!
