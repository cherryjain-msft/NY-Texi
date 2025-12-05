# GM Data Analysis - Medallion Architecture Implementation Guide

## 🎯 Overview

This project implements a complete medallion architecture (Bronze → Silver → Gold) for analyzing GM data from 2023. The implementation follows Databricks best practices for Delta Lake, Unity Catalog, and data quality management.

## 📁 Project Structure

```
NY Texi/
├── databricks.yml                 # Main Databricks bundle configuration
├── notebooks/
│   └── 01_data_discovery.py       # Data discovery and exploration notebook
├── src/
│   └── gm_analysis/
│       ├── bronze/
│       │   └── ingest_data.py     # Bronze layer ingestion module
│       ├── silver/
│       │   └── cleanse_data.py    # Silver layer transformation module
│       ├── gold/
│       │   └── aggregate_metrics.py # Gold layer aggregation module
│       ├── analytics/
│       │   └── visualize_metrics.py # Visual analytics module
│       └── sql/
│           ├── create_bronze_tables.sql  # Bronze DDL
│           ├── create_silver_tables.sql  # Silver DDL
│           ├── create_gold_tables.sql    # Gold DDL
│           ├── grants.sql                # Unity Catalog grants
│           ├── optimize.py               # Delta optimization module
│           └── validate_delta.py         # Data quality validation
└── resources/
    ├── workflows.yml              # Databricks workflow definitions
    └── unity_catalog.yml          # Unity Catalog configuration
```

## 🚀 Quick Start

### Step 1: Data Discovery

Run the data discovery notebook to understand your data:

```bash
databricks notebook run notebooks/01_data_discovery.py --parameters year=2023
```

This will:
- List all files in `/Volumes/gm_demo/gm_test_schema/gm_volume/2023/`
- Identify data formats (CSV, Parquet, JSON, etc.)
- Analyze schemas and data quality
- Recommend partitioning strategies

### Step 2: Configure Your Environment

Update `databricks.yml` with your settings:

```yaml
variables:
  catalog_name: "gm_demo"
  warehouse_id: "your-warehouse-id"
  cluster_node_type: "Standard_DS4_v2"
```

### Step 3: Run the Medallion Pipeline

The pipeline consists of three main layers:

#### 🥉 Bronze Layer - Raw Data Ingestion

```python
from src.gm_analysis.bronze.ingest_data import ingest_year_data

ingest_year_data(spark, year="2023")
```

**Bronze Layer Features:**
- Preserves raw data exactly as-is
- Adds metadata columns (`_ingestion_timestamp`, `_source_file`, `_source_modified_time`)
- ACID guarantees with Delta Lake
- Change Data Feed enabled
- Append-only architecture

#### 🥈 Silver Layer - Data Cleansing

```python
from src.gm_analysis.silver.cleanse_data import transform_bronze_to_silver

transform_bronze_to_silver(
    spark=spark,
    bronze_table="gm_data_2023",
    silver_table="gm_data_2023",
    key_columns=["id"],  # Update with your key columns
    partition_cols=["processing_date"]
)
```

**Silver Layer Features:**
- Deduplication based on key columns
- Null handling and data standardization
- Business rule validation
- Data quality metrics (`_completeness_score`, `_null_column_count`)
- SCD Type 2 support (optional)

#### 🥇 Gold Layer - Business Aggregates

```python
from src.gm_analysis.gold.aggregate_metrics import create_gold_aggregations

create_gold_aggregations(
    spark=spark,
    silver_table="gm_data_2023",
    date_column="transaction_date",
    metric_columns=["amount", "quantity"],
    dimension_columns=["category", "region"]
)
```

**Gold Layer Features:**
- Daily and monthly aggregations
- Pre-computed business metrics
- Dimension tables for star schema
- Z-ordered for query performance
- Optimized for analytics workloads

### Step 4: Data Quality Validation

```python
from src.gm_analysis.sql.validate_delta import run_full_validation_suite

run_full_validation_suite(spark, year="2023")
```

**Validation Checks:**
- Row count consistency across layers
- Schema validation
- Null percentage checks
- Duplicate detection
- Data freshness monitoring

### Step 5: Optimize Delta Tables

```python
from src.gm_analysis.sql.optimize import run_maintenance_job

run_maintenance_job(
    spark=spark,
    year="2023",
    vacuum=True,
    retention_hours=168  # 7 days
)
```

**Optimization Operations:**
- `OPTIMIZE` with Z-ordering
- `VACUUM` to remove old files
- `ANALYZE TABLE` for statistics
- Auto-optimization enabled

### Step 6: Create Visualizations

```python
from src.gm_analysis.analytics.visualize_metrics import create_visualizations

create_visualizations(spark, year="2023")
```

**Available Dashboards:**
- Data volume trends
- Data quality metrics
- Layer comparison charts
- Business metrics visualizations

## 🔐 Unity Catalog Setup

### Register Tables

```sql
-- Run grants to set up permissions
source src/gm_analysis/sql/grants.sql
```

**Access Control Hierarchy:**
- **Bronze Layer**: Data Engineering only
- **Silver Layer**: Data Engineering (read/write), Analytics (read)
- **Gold Layer**: All users (read), Data Engineering (write)

### Table Tags

All tables are tagged with metadata:
- `layer`: bronze/silver/gold
- `data_classification`: raw/cleansed/aggregated
- `business_critical`: true/false
- `retention_policy`: Duration for data retention

## 📊 Workflows and Orchestration

Deploy the workflow using Databricks Asset Bundles:

```bash
databricks bundle deploy --target production
databricks bundle run gm_medallion_pipeline_2023
```

**Pipeline Tasks:**
1. Data Discovery
2. Bronze Ingestion
3. Silver Transformation
4. Data Validation
5. Gold Aggregation
6. Table Optimization
7. Dashboard Creation

**Schedule:**
- Main pipeline: Daily at 2 AM UTC
- Maintenance job: Weekly on Sunday at 3 AM UTC

## 🎯 Best Practices Implemented

### Delta Lake
✅ Partitioning by date for time-series data  
✅ Z-ordering on frequently filtered columns  
✅ Auto-optimization enabled  
✅ Change Data Feed for lineage  
✅ Regular OPTIMIZE and VACUUM operations  

### Data Quality
✅ Comprehensive validation framework  
✅ Data quality metrics at every layer  
✅ Automated testing and monitoring  
✅ Anomaly detection  

### Security & Governance
✅ Unity Catalog integration  
✅ Fine-grained access control  
✅ Table and column tagging  
✅ Audit trail with metadata columns  

### Performance
✅ Appropriate partitioning strategy  
✅ Z-ordering for query optimization  
✅ Caching frequently accessed data  
✅ Statistics collection  

## 📝 Customization Guide

### Update Column Mappings

After running data discovery, update these files with your actual column names:

1. **Bronze Ingestion** (`src/gm_analysis/bronze/ingest_data.py`):
   - Update `partition_cols` based on your date columns

2. **Silver Transformation** (`src/gm_analysis/silver/cleanse_data.py`):
   - Update `key_columns` with your unique identifiers
   - Configure `null_handling_rules` for your business logic

3. **Gold Aggregation** (`src/gm_analysis/gold/aggregate_metrics.py`):
   - Set `date_column` to your date/timestamp field
   - Define `metric_columns` (numeric fields to aggregate)
   - Specify `dimension_columns` (categorical fields)

4. **Data Validation** (`src/gm_analysis/sql/validate_delta.py`):
   - Configure `column_thresholds` for null validation
   - Set `key_columns` for duplicate detection

## 🔍 Monitoring & Troubleshooting

### View Table Details

```python
from src.gm_analysis.sql.optimize import DeltaOptimizer

optimizer = DeltaOptimizer(spark)
optimizer.get_table_details("bronze_gm_data_2023")
```

### Check Data Quality Results

```sql
SELECT * FROM gm_demo.gm_test_schema.data_quality_validations
ORDER BY timestamp DESC
LIMIT 10;
```

### Monitor Workflow Status

```bash
databricks jobs list-runs --job-id <job-id> --limit 10
```

## 📈 Performance Tuning

### Recommended Cluster Configuration

**Development:**
- Node type: `Standard_DS3_v2`
- Workers: 2-4
- Autoscaling: Enabled

**Production:**
- Node type: `Standard_DS4_v2` or larger
- Workers: 4-8
- Autoscaling: Enabled with max 20 workers
- Spot instances: 80% for cost optimization

### Query Optimization Tips

1. **Filter early**: Apply WHERE clauses before JOINs
2. **Use partition pruning**: Include partition columns in WHERE
3. **Cache intermediate results**: For iterative queries
4. **Broadcast small tables**: Use `broadcast()` hint
5. **Enable AQE**: Adaptive Query Execution for dynamic optimization

## 🆘 Common Issues

### Issue: "Table not found"
**Solution**: Ensure Unity Catalog is properly configured and you have USE CATALOG permission

### Issue: "File already exists" during ingestion
**Solution**: Use `mode="append"` or set up idempotent ingestion with COPY INTO

### Issue: "Out of memory"
**Solution**: Increase cluster size, enable auto-scaling, or partition data more granularly

### Issue: "Slow query performance"
**Solution**: Run OPTIMIZE with Z-ordering, ensure statistics are up-to-date

## 📚 Additional Resources

- [Databricks Delta Lake Best Practices](https://docs.databricks.com/delta/best-practices.html)
- [Medallion Architecture Guide](https://www.databricks.com/glossary/medallion-architecture)
- [Unity Catalog Documentation](https://docs.databricks.com/data-governance/unity-catalog/index.html)
- [Delta Lake Optimization](https://docs.databricks.com/delta/optimize.html)

## 🤝 Contributing

To add new features or modify existing ones:

1. Update the appropriate module in `src/gm_analysis/`
2. Add tests for your changes
3. Update this README with any new functionality
4. Run data validation to ensure quality

## 📧 Support

For issues or questions:
- Data Engineering Team: data-engineering@company.com
- Alerts: alerts@company.com

---

**Last Updated**: December 2025  
**Version**: 1.0.0  
**Maintainer**: Data Engineering Team
