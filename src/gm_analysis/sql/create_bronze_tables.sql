-- Bronze Layer DDL
-- Creates bronze tables for raw data ingestion

-- Enable Change Data Feed for all bronze tables
SET spark.databricks.delta.properties.defaults.enableChangeDataFeed = true;

-- Bronze table for GM data 2023
CREATE TABLE IF NOT EXISTS gm_test_schema.bronze_gm_data_2023 (
  -- Original data columns will be inferred from source
  -- Metadata columns
  _ingestion_timestamp TIMESTAMP COMMENT 'Timestamp when data was ingested',
  _source_file STRING COMMENT 'Source file path',
  _source_modified_time TIMESTAMP COMMENT 'Source file modification time'
)
USING DELTA
PARTITIONED BY (_ingestion_date DATE)
LOCATION 'dbfs:/user/hive/warehouse/gm_test_schema.db/bronze_gm_data_2023'
COMMENT 'Bronze layer - Raw GM data for 2023. Append-only table with audit metadata.'
TBLPROPERTIES (
  'delta.enableChangeDataFeed' = 'true',
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact' = 'true',
  'delta.deletedFileRetentionDuration' = 'interval 7 days',
  'delta.logRetentionDuration' = 'interval 30 days',
  'quality_tier' = 'bronze',
  'data_classification' = 'raw',
  'owner' = 'data_engineering'
);

-- Generic bronze table template (copy and customize for each dataset)
-- CREATE TABLE IF NOT EXISTS gm_test_schema.bronze_{table_name} (
--   -- Source columns (inferred or specified)
--   _ingestion_timestamp TIMESTAMP,
--   _source_file STRING,
--   _source_modified_time TIMESTAMP
-- )
-- USING DELTA
-- PARTITIONED BY ({partition_column})
-- TBLPROPERTIES (
--   'delta.enableChangeDataFeed' = 'true',
--   'delta.autoOptimize.optimizeWrite' = 'true',
--   'delta.autoOptimize.autoCompact' = 'true'
-- );
