# NYC Taxi Data - Databricks Asset Bundle

Convert 15 years of NYC taxi parquet data into Unity Catalog Delta tables using Databricks Asset Bundles.

## Project Structure

```
NY Texi/
├── databricks.yml              # Main bundle configuration
├── variables.yml               # Environment variables (deprecated - now in databricks.yml)
├── resources/
│   ├── unity_catalog.yml       # Catalog, schema, volume definitions
│   └── workflows.yml           # Job and task definitions
├── src/
│   ├── ingestion/
│   │   ├── batch_ingest.py           # Main ingestion notebook
│   │   └── ingest_taxi_zones.py      # Taxi zones reference data
│   ├── quality/
│   │   └── validate_delta.py         # Data quality checks
│   ├── optimization/
│   │   └── optimize.py               # OPTIMIZE, VACUUM, ANALYZE
│   └── sql/
│       ├── create_tables.sql         # Table DDL
│       └── create_curated_tables.sql # Silver/Gold layer
└── README.md
```

## Data Overview

- **Source**: Unity Catalog Volume `/Volumes/gm_demo/default/ny_texi`
- **Years**: 2009-2023 (15 years)
- **Files**: 443 parquet files
- **Taxi Types**: Yellow, Green, FHV, HVFHS
- **Reference Data**: 265 taxi zones with shapefiles

## Setup Instructions

### 1. Configure Variables

Edit `databricks.yml` and set the following:

```yaml
warehouse_id: "<your-sql-warehouse-id>"
notification_email: "<your-email>"
storage_account: "<your-storage-account>"  # For production
```

### 2. Deploy the Bundle

```bash
# Validate configuration
databricks bundle validate -t dev

# Deploy to development workspace
databricks bundle deploy -t dev
```

### 3. Create Unity Catalog Resources

The bundle will use existing catalog and create:
- Catalog: `gm_demo` (already exists)
- Schemas: `raw`, `curated`, `analytics`
- Source Volume: `gm_demo.default.ny_texi` (already exists with data)
- Volumes: `taxi_checkpoints`, `taxi_artifacts`

### 4. Create Tables

Run the DDL to create table structures:

```bash
databricks workspace import src/sql/create_tables.sql
```

Or execute directly in SQL Warehouse.

### 5. Run Data Ingestion

```bash
# Run the full ingestion workflow
databricks bundle run taxi_data_ingestion -t dev
```

The workflow will:
1. Ingest taxi zones reference data
2. Ingest yellow taxi data (2009-2016) in parallel
3. Ingest yellow taxi data (2017-2023) in parallel
4. Ingest green taxi data (2014-2023)
5. Ingest FHV data (2015-2023)
6. Ingest HVFHS data (2019-2023)
7. Run data quality validation
8. Optimize Delta tables
9. Create curated layer tables

## Data Pipeline Layers

### Bronze Layer (Raw)
- `raw.yellow_taxi_trips` - Yellow taxi trip records
- `raw.green_taxi_trips` - Green taxi trip records
- `raw.fhv_trips` - For-hire vehicle trips
- `raw.hvfhs_trips` - High-volume FHV trips (Uber/Lyft)
- `raw.taxi_zones` - Taxi zone reference data

### Silver Layer (Curated)
- `curated.yellow_taxi_trips` - Cleaned, enriched yellow taxi data
- `curated.green_taxi_trips` - Cleaned, enriched green taxi data

Features:
- Zone names joined from reference data
- Derived columns (trip duration, tip percentage, avg speed)
- Data quality flags
- Payment/rate code descriptions

### Gold Layer (Analytics)
- `analytics.monthly_trip_summary` - Monthly aggregations by taxi type
- `analytics.popular_routes` - Most frequented pickup/dropoff combinations
- `analytics.hourly_patterns` - Trip patterns by hour and day type

## Manual Operations

### Run Individual Tasks

```bash
# Ingest specific taxi type
databricks jobs run-now --job-id <job-id> \
  --notebook-params '{"taxi_type":"yellow_taxi","start_year":"2020","end_year":"2023"}'

# Run data quality checks
databricks jobs run-now --job-id <job-id> --task-key validate_data_quality

# Optimize tables
databricks jobs run-now --job-id <job-id> --task-key optimize_tables
```

### Query Data

```sql
-- Check ingestion counts
SELECT 'yellow_taxi_trips' AS table_name, COUNT(*) AS row_count 
FROM gm_demo.raw.yellow_taxi_trips
UNION ALL
SELECT 'green_taxi_trips', COUNT(*) 
FROM gm_demo.raw.green_taxi_trips;

-- Monthly summary
SELECT * FROM gm_demo.analytics.monthly_trip_summary
ORDER BY year DESC, month DESC
LIMIT 12;

-- Popular routes
SELECT * FROM gm_demo.analytics.popular_routes
WHERE year = 2023
ORDER BY trip_count DESC
LIMIT 20;
```

## Optimization

### Z-ORDERING

Tables are automatically Z-ordered on:
- Yellow/Green: `PULocationID`, `pickup_datetime`
- FHV/HVFHS: `PUlocationID`, `pickup_datetime`
- Taxi Zones: `LocationID`

### Auto-Optimization

All tables have:
```sql
'delta.autoOptimize.optimizeWrite' = 'true'
'delta.autoOptimize.autoCompact' = 'true'
```

### Manual Optimization

```bash
# Run optimize on all tables
databricks bundle run taxi_data_ingestion -t dev --task optimize_tables
```

Or via SQL:
```sql
OPTIMIZE gm_demo.raw.yellow_taxi_trips
ZORDER BY (PULocationID, tpep_pickup_datetime);

VACUUM gm_demo.raw.yellow_taxi_trips RETAIN 168 HOURS;
```

## Monitoring

### Job Status

Monitor in Databricks UI:
- Workflows → `nyc_taxi_ingestion_dev`
- View task history, logs, and metrics

### Table Statistics

```sql
DESCRIBE DETAIL gm_demo.raw.yellow_taxi_trips;

SELECT 
  format, 
  numFiles, 
  sizeInBytes / 1024 / 1024 / 1024 AS sizeInGB,
  properties
FROM (DESCRIBE DETAIL gm_demo.raw.yellow_taxi_trips);
```

### Data Quality Reports

Check validation output in workflow logs for:
- Row counts
- Null value percentages
- Date range validation
- Negative value detection

## Troubleshooting

### Common Issues

1. **"Table already exists"**
   - Use `CREATE OR REPLACE TABLE` or drop existing tables

2. **Schema evolution errors**
   - Ensure `spark.databricks.delta.schema.autoMerge.enabled = true`
   - Add `.option("mergeSchema", "true")` to write operations

3. **Permission errors**
   - Verify Unity Catalog permissions for catalog/schema
   - Check cluster has access to source data path

4. **Out of memory**
   - Increase cluster size or max_workers
   - Reduce partition size in spark config

### Logs

View logs in:
- Databricks UI → Workflows → Job Runs → Task Logs
- Structured JSON logs in notebook output

## Production Deployment

1. Upload data to ADLS Gen2
2. Update `prod` target in `databricks.yml`:
   ```yaml
   prod:
     variables:
       source_data_path: "abfss://taxi-landing@storage.dfs.core.windows.net/Dataset"
       catalog_name: prod_nyc_taxi_data
   ```
3. Deploy: `databricks bundle deploy -t prod`
4. Run: `databricks bundle run taxi_data_ingestion -t prod`

## Next Steps

1. Set up incremental ingestion for new monthly data
2. Create dashboards in Databricks SQL or Power BI
3. Implement ML models for demand forecasting
4. Add CDC (Change Data Capture) for real-time updates
5. Set up data lineage tracking

## Resources

- [Databricks Asset Bundles Documentation](https://docs.databricks.com/dev-tools/bundles/index.html)
- [Unity Catalog](https://docs.databricks.com/data-governance/unity-catalog/index.html)
- [Delta Lake](https://docs.delta.io/)
- [NYC TLC Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)
