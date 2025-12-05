# GM Data Analysis - Medallion Architecture Prompt

## Objective
Analyze the specified year folder of `/Volumes/gm_demo/gm_test_schema/gm_volume/{year}` using existing files as context, and create a logical medallion architecture (Bronze, Silver, Gold) with Delta tables registered under `gm_test_schema`. Include basic visual analytics.

## Parameters
- **year**: The year folder to process (e.g., 2023, 2024)

## Context Files
- Review existing workflow patterns in `resources/workflows.yml`
- Reference Unity Catalog structure in `resources/unity_catalog.yml`
- Examine ingestion patterns in `src/ingestion/`
- Study optimization techniques in `src/optimization/optimize.py`
- Review data quality validation in `src/quality/validate_delta.py`
- Use SQL patterns from `src/sql/` for table creation

## Requirements

### 1. Data Discovery & Analysis
- [ ] List and explore contents of `/Volumes/gm_demo/gm_test_schema/gm_volume/{year}/`
- [ ] Identify data formats (CSV, Parquet, JSON, etc.)
- [ ] Analyze schema structure and data types
- [ ] Identify data quality issues and patterns
- [ ] Document data volume and partitioning strategy

### 2. Bronze Layer (Raw Data)
**Purpose**: Ingest raw data exactly as-is for audit and reprocessing

- [ ] Create bronze Delta tables in `gm_test_schema.bronze_*` 
- [ ] Preserve original data format and structure
- [ ] Add metadata columns:
  - `_ingestion_timestamp` - When data was loaded
  - `_source_file` - Source file path
  - `_source_modified_time` - File modification timestamp
- [ ] Implement incremental ingestion pattern
- [ ] Enable Change Data Feed for downstream processing
- [ ] Set appropriate table properties (retention, optimization)

**Best Practices**:
- Use COPY INTO or Auto Loader for scalable ingestion
- Partition by ingestion date for efficient querying
- Store data in Delta format for ACID guarantees
- Never delete or modify bronze data - append only

### 3. Silver Layer (Cleansed & Conformed)
**Purpose**: Clean, deduplicate, and standardize data

- [ ] Create silver Delta tables in `gm_test_schema.silver_*`
- [ ] Data quality transformations:
  - Remove duplicates
  - Handle null values appropriately
  - Standardize data types and formats
  - Apply business rules and validations
- [ ] Add data quality metrics columns
- [ ] Implement SCD Type 2 if needed for historical tracking
- [ ] Create appropriate indexes and Z-ordering
- [ ] Document data lineage

**Best Practices**:
- Use Delta Merge for upserts
- Implement data quality checks before promotion
- Add constraints and expectations using Delta Live Tables expectations
- Partition by business-relevant columns (e.g., date, region)
- Enable Z-ordering on commonly filtered columns

### 4. Gold Layer (Business-Level Aggregates)
**Purpose**: Create business-ready, aggregated datasets

- [ ] Create gold Delta tables in `gm_test_schema.gold_*`
- [ ] Build aggregated views:
  - Time-series aggregations (daily, monthly, yearly)
  - Business metrics and KPIs
  - Dimensional models (facts and dimensions)
- [ ] Optimize for query performance
- [ ] Add business-friendly column names and descriptions
- [ ] Implement appropriate access controls

**Best Practices**:
- Denormalize for query performance
- Pre-compute expensive aggregations
- Use materialized views or tables based on query patterns
- Add comprehensive table and column comments
- Consider using Liquid Clustering for frequently changing filter patterns

### 5. Data Quality & Validation
- [ ] Implement row count validation across layers
- [ ] Add schema evolution handling
- [ ] Create data quality dashboards
- [ ] Set up alerts for data anomalies
- [ ] Document data quality rules

**Reference**: Use patterns from `src/quality/validate_delta.py`

### 6. Optimization Strategy
- [ ] Run OPTIMIZE on all tables after initial load
- [ ] Set up maintenance jobs:
  - OPTIMIZE with Z-ordering
  - VACUUM to remove old files (respect retention period)
  - ANALYZE TABLE for statistics
- [ ] Configure auto-optimization where applicable

**Reference**: Use patterns from `src/optimization/optimize.py`

### 7. Visual Analytics
Create basic visualizations for:

- [ ] **Data Volume Trends**
  - Records per table over time
  - Data growth rate
  - Ingestion success/failure rates

- [ ] **Data Quality Metrics**
  - Null value percentages by column
  - Duplicate record counts
  - Schema drift detection

- [ ] **Business Metrics** (based on discovered data)
  - Key aggregations by time period
  - Dimensional breakdowns
  - Trend analysis

**Tools**: Use Databricks SQL dashboards or notebooks with matplotlib/plotly

### 8. Workflow & Orchestration
- [ ] Create Databricks workflow in `resources/workflows.yml`
- [ ] Define job dependencies (Bronze → Silver → Gold)
- [ ] Set up scheduling (daily, hourly, etc.)
- [ ] Configure retry policies and error handling
- [ ] Add job monitoring and notifications

**Reference**: Follow patterns in `resources/workflows.yml`

### 9. Unity Catalog Registration
- [ ] Register all Delta tables in Unity Catalog
- [ ] Set appropriate table properties
- [ ] Add table and column comments
- [ ] Configure access controls (GRANT statements)
- [ ] Tag tables with appropriate metadata

**Reference**: Use patterns from `resources/unity_catalog.yml`

## Best Practices Summary

### Delta Lake Best Practices
1. **Partitioning**: Use date-based partitioning for time-series data, avoid over-partitioning (target 1GB+ per partition)
2. **Z-Ordering**: Apply to columns frequently used in WHERE clauses
3. **Optimization**: Run OPTIMIZE regularly, especially after heavy writes
4. **Vacuum**: Run with appropriate retention (default 7 days) to clean up old files
5. **Table Properties**:
   ```sql
   delta.autoOptimize.optimizeWrite = true
   delta.autoOptimize.autoCompact = true
   delta.enableChangeDataFeed = true
   ```

### Schema Management
1. Use schema evolution carefully with `mergeSchema` option
2. Document all schema changes
3. Version control DDL statements
4. Use constraints to enforce data quality

### Performance Optimization
1. **Caching**: Cache frequently accessed tables
2. **Broadcasting**: Use broadcast joins for small dimension tables
3. **Adaptive Query Execution**: Enable AQE for dynamic optimization
4. **Column Pruning**: Select only needed columns
5. **Predicate Pushdown**: Filter data as early as possible

### Data Governance
1. Use Unity Catalog for centralized governance
2. Implement column-level and row-level security
3. Tag sensitive data appropriately
4. Maintain data lineage documentation
5. Regular access reviews

### Error Handling
1. Implement comprehensive logging
2. Use try-catch blocks for graceful failures
3. Set up alerting for job failures
4. Maintain audit trails
5. Version control all code

### Documentation
1. Add comments to all tables and columns
2. Document business logic in code
3. Maintain runbooks for operational procedures
4. Create data dictionaries
5. Document data lineage and transformations

## Code Structure

### Recommended File Organization
```
src/
  gm_analysis/
    bronze/
      ingest_data.py               # Bronze layer ingestion
    silver/
      cleanse_data.py              # Silver layer transformations
    gold/
      aggregate_metrics.py         # Gold layer aggregations
    analytics/
      visualize_metrics.py         # Visual analytics
    sql/
      create_bronze_tables.sql     # DDL for bronze tables
      create_silver_tables.sql     # DDL for silver tables
      create_gold_tables.sql       # DDL for gold tables
      grants.sql                   # Access control
```

## Testing Strategy
- [ ] Unit tests for transformation logic
- [ ] Integration tests for end-to-end pipeline
- [ ] Data quality tests with expectations
- [ ] Performance benchmarks
- [ ] Validate counts at each layer

## Monitoring & Observability
- [ ] Track job execution metrics
- [ ] Monitor data freshness
- [ ] Alert on SLA violations
- [ ] Dashboard for pipeline health
- [ ] Cost tracking and optimization

## Deliverables
1. Three-layer medallion architecture (Bronze, Silver, Gold)
2. All tables registered in Unity Catalog under `gm_test_schema`
3. Automated workflows for data processing
4. Data quality validation framework
5. Visual analytics dashboards
6. Documentation and runbooks

## Success Criteria
- All data for specified year successfully loaded into bronze layer
- Data quality checks passing with >95% accuracy
- Silver layer free of duplicates and null key fields
- Gold layer optimized for sub-second query performance
- Visual dashboards accessible to business users
- Complete documentation and lineage tracking

## References
- [Delta Lake Best Practices](https://docs.databricks.com/delta/best-practices.html)
- [Medallion Architecture](https://www.databricks.com/glossary/medallion-architecture)
- [Unity Catalog Guide](https://docs.databricks.com/data-governance/unity-catalog/index.html)
- [Delta Live Tables](https://docs.databricks.com/workflows/delta-live-tables/index.html)
