# Plan: NYC Taxi Data to Unity Catalog Delta Tables via Databricks Asset Bundle

Transform 15 years of NYC taxi parquet data (443 files, 2009-2023) into optimized Unity Catalog Delta tables using Databricks Asset Bundles for automated deployment and orchestration.

## Steps

### 1. Set up Unity Catalog structure
**File**: `resources/unity_catalog.yml`

- Use existing catalog `gm_demo` with schemas: `raw` (bronze), `curated` (silver), `analytics` (gold)
- Source data is in Unity Catalog volume `/Volumes/gm_demo/default/ny_texi`
- Configure managed volumes for checkpoints and artifacts
- Set up grants for data_engineers group

### 2. Define ingestion workflows
**File**: `resources/workflows.yml`

- Create multi-task job with parallel ingestion by year ranges (2009-2016, 2017-2023)
- Configure optimized clusters (memory-optimized nodes, autoscaling 2-8 workers)
- Add task dependencies: validation → ingestion → quality checks → curated layer → optimization
- Enable retry logic (3 attempts, 5-min intervals) and timeout handling (2-hour limits)

### 3. Build ingestion notebooks
**Directory**: `src/ingestion/`

- Create `batch_ingest.py` to read parquet files and write to Delta tables partitioned by (year, month)
- Implement schema evolution handling with `mergeSchema` option
- Add structured logging for metrics (rows processed, duration, files ingested)
- Create separate ingestion logic for each taxi type: `yellow_taxi`, `green_taxi`, `fhv`, `hvfhs`

### 4. Create Delta table schemas
**File**: `src/sql/create_tables.sql`

- Define tables for 4 taxi types + taxi_zones reference table
- Enable `delta.autoOptimize.optimizeWrite` and `delta.autoOptimize.autoCompact`
- Configure log retention (90 days) and deleted file retention (7 days)
- Set up Z-ordering on frequently queried columns (`pickup_location_id`, `tpep_pickup_datetime`)

### 5. Implement data quality & optimization tasks

- Build `src/quality/validate_delta.py` for row counts, null checks, date range validation
- Create `src/optimization/optimize.py` for scheduled OPTIMIZE and VACUUM operations
- Add curated layer transformations in `src/sql/create_curated_tables.sql` (joins with taxi_zones, deduplication, derived columns)

### 6. Configure deployment variables and deploy bundle

- Update `databricks.yml` with includes for resource files and variables
- Set catalog names, warehouse IDs, storage paths
- Deploy with `databricks bundle deploy -t dev` and validate resources
- Execute initial ingestion workflow with `databricks bundle run taxi_data_ingestion -t dev`

## Implementation Status

✅ **Completed**: All steps implemented
- Project structure created
- Unity Catalog resources configured
- Ingestion workflows defined
- Data quality and optimization notebooks created
- Curated layer SQL transformations defined
- Main configuration updated

## Data Overview

### Dataset Structure
- **Location**: `/Volumes/gm_demo/default/ny_texi` (Unity Catalog Volume)
- **Time Range**: 2009-2023 (15 years)
- **File Count**: 443 parquet files + reference data
- **Taxi Types**: Yellow, Green, FHV, HVFHS

### Reference Data
- `02.taxi_zones/taxi+_zone_lookup.csv` - 265 taxi zones with LocationID, Borough, Zone name
- Data dictionaries (PDF) for each taxi type
- Geographic shapefiles and zone maps

### Partitioning Strategy
- **Partition by**: year, month (180 partitions for 15 years)
- **Target partition size**: 100MB - 1GB per partition
- Avoids over-partitioning and small files

## Unity Catalog Configuration

### Catalog and Schemas
- **Catalog**: `gm_demo` (existing)
- **Schemas**:
  - `raw` - Bronze layer for ingested parquet data
  - `curated` - Silver layer for cleaned/enriched data
  - `analytics` - Gold layer for aggregated metrics

### Volumes
- **Source Data**: `gm_demo.default.ny_texi` (existing, contains parquet files)
- **Checkpoints**: `gm_demo.raw.taxi_checkpoints` (managed)
- **Artifacts**: `gm_demo.raw.taxi_artifacts` (managed)

## Deployment Steps

### 1. Prerequisites
- Databricks workspace access
- Unity Catalog enabled
- Data already in volume `gm_demo.default.ny_texi`
- SQL Warehouse created

### 2. Configure Settings
Edit `databricks.yml` and set:
```yaml
warehouse_id: "<your-sql-warehouse-id>"
notification_email: "<your-email>"
```

### 3. Deploy Bundle
```bash
# Validate configuration
databricks bundle validate -t dev

# Deploy to workspace
databricks bundle deploy -t dev
```

### 4. Create Tables
Execute the DDL:
```bash
# Run via SQL Warehouse or notebook
databricks workspace import src/sql/create_tables.sql
```

### 5. Run Ingestion
```bash
# Execute the full pipeline
databricks bundle run taxi_data_ingestion -t dev
```

## Pipeline Workflow

The ingestion workflow executes these tasks in order:

1. **ingest_taxi_zones** - Load reference data from `02.taxi_zones/taxi+_zone_lookup.csv`
2. **ingest_yellow_2009_2016** - Parallel ingestion of yellow taxi (2009-2016)
3. **ingest_yellow_2017_2023** - Parallel ingestion of yellow taxi (2017-2023)
4. **ingest_green_taxi** - Ingest green taxi (2014-2023)
5. **ingest_fhv** - Ingest FHV (2015-2023)
6. **ingest_hvfhs** - Ingest HVFHS (2019-2023)
7. **validate_data_quality** - Run quality checks on all tables
8. **optimize_tables** - OPTIMIZE with Z-ORDERING
9. **create_curated_layer** - Create silver/gold layer tables

## Data Quality Checks

Automated validation includes:
- Row count verification
- Null value detection in critical columns
- Date range validation (2009-2024)
- Negative value detection in amount/distance fields
- Trip duration validation

## Optimization Strategy

### Auto-Optimization
All tables configured with:
```sql
'delta.autoOptimize.optimizeWrite' = 'true'
'delta.autoOptimize.autoCompact' = 'true'
```

### Z-ORDERING
Tables optimized on frequently queried columns:
- Yellow/Green: `PULocationID`, `pickup_datetime`
- FHV/HVFHS: `PUlocationID`, `pickup_datetime`
- Taxi Zones: `LocationID`

### Retention Policies
- Transaction log: 90 days
- Deleted files: 7 days
- VACUUM runs after optimization

## Analytics Tables

### Gold Layer Tables
1. **monthly_trip_summary** - Trip counts, revenue, averages by month and taxi type
2. **popular_routes** - Most frequent pickup/dropoff combinations
3. **hourly_patterns** - Trip patterns by hour and weekday/weekend

## Next Steps

1. ✅ Review and configure `databricks.yml` settings
2. ⏭️ Deploy bundle to Databricks workspace
3. ⏭️ Execute ingestion workflow
4. ⏭️ Validate data in Unity Catalog tables
5. ⏭️ Create dashboards and visualizations
6. ⏭️ Set up incremental processing for new data
7. ⏭️ Implement ML models for demand forecasting

## Resources

- [Databricks Asset Bundles Documentation](https://docs.databricks.com/dev-tools/bundles/index.html)
- [Unity Catalog](https://docs.databricks.com/data-governance/unity-catalog/index.html)
- [Delta Lake Optimization](https://docs.delta.io/latest/optimizations-oss.html)
- [NYC TLC Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)
