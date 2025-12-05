-- Gold Layer DDL
-- Creates gold tables for business-ready analytics

-- Daily aggregation table
CREATE TABLE IF NOT EXISTS gm_test_schema.gold_gm_data_2023_daily (
  agg_date DATE COMMENT 'Aggregation date',
  -- Dimension columns (to be defined based on data)
  -- Aggregated metrics (to be defined based on data)
  record_count BIGINT COMMENT 'Number of records in aggregation',
  _gold_processed_timestamp TIMESTAMP COMMENT 'Processing timestamp',
  aggregation_level STRING COMMENT 'Level of aggregation (daily, monthly, yearly)'
)
USING DELTA
PARTITIONED BY (agg_date)
LOCATION 'dbfs:/user/hive/warehouse/gm_test_schema.db/gold_gm_data_2023_daily'
COMMENT 'Gold layer - Daily aggregated GM metrics for 2023. Optimized for analytics.'
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact' = 'true',
  'delta.deletedFileRetentionDuration' = 'interval 7 days',
  'quality_tier' = 'gold',
  'data_classification' = 'aggregated',
  'owner' = 'analytics'
);

-- Monthly aggregation table
CREATE TABLE IF NOT EXISTS gm_test_schema.gold_gm_data_2023_monthly (
  agg_year INT COMMENT 'Aggregation year',
  agg_month INT COMMENT 'Aggregation month',
  -- Dimension columns (to be defined based on data)
  -- Aggregated metrics (to be defined based on data)
  record_count BIGINT COMMENT 'Number of records in aggregation',
  _gold_processed_timestamp TIMESTAMP COMMENT 'Processing timestamp',
  aggregation_level STRING COMMENT 'Level of aggregation (daily, monthly, yearly)'
)
USING DELTA
PARTITIONED BY (agg_year, agg_month)
LOCATION 'dbfs:/user/hive/warehouse/gm_test_schema.db/gold_gm_data_2023_monthly'
COMMENT 'Gold layer - Monthly aggregated GM metrics for 2023. Optimized for trend analysis.'
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact' = 'true',
  'quality_tier' = 'gold',
  'data_classification' = 'aggregated',
  'owner' = 'analytics'
);

-- Dimension table
CREATE TABLE IF NOT EXISTS gm_test_schema.gold_gm_data_2023_dimensions (
  dimension_key BIGINT COMMENT 'Surrogate key for dimension',
  -- Dimension columns (to be defined based on data)
  record_count BIGINT COMMENT 'Number of records for this dimension combination'
)
USING DELTA
LOCATION 'dbfs:/user/hive/warehouse/gm_test_schema.db/gold_gm_data_2023_dimensions'
COMMENT 'Gold layer - Dimension table for GM data 2023.'
TBLPROPERTIES (
  'quality_tier' = 'gold',
  'data_classification' = 'dimension',
  'owner' = 'analytics'
);

-- Example KPI table structure
-- CREATE TABLE IF NOT EXISTS gm_test_schema.gold_gm_kpis_2023 (
--   kpi_date DATE,
--   kpi_name STRING,
--   kpi_value DOUBLE,
--   kpi_target DOUBLE,
--   variance DOUBLE,
--   variance_pct DOUBLE,
--   _gold_processed_timestamp TIMESTAMP
-- )
-- USING DELTA
-- PARTITIONED BY (kpi_date)
-- COMMENT 'Gold layer - KPI tracking for GM data 2023';
