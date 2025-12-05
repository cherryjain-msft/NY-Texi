-- Silver Layer DDL
-- Creates silver tables for cleansed and standardized data

-- Silver table for GM data 2023
CREATE TABLE IF NOT EXISTS gm_test_schema.silver_gm_data_2023 (
  -- Business columns (will be defined based on bronze schema)
  -- Data quality metadata
  _is_valid BOOLEAN COMMENT 'Indicates if record passed all validation rules',
  _validation_errors STRING COMMENT 'Description of any validation failures',
  _null_column_count INT COMMENT 'Number of null columns in this record',
  _completeness_score DOUBLE COMMENT 'Data completeness score (0-1)',
  _silver_processed_timestamp TIMESTAMP COMMENT 'Timestamp when record was processed to silver',
  -- Bronze lineage
  _ingestion_timestamp TIMESTAMP COMMENT 'Original bronze ingestion timestamp',
  _source_file STRING COMMENT 'Original source file path'
)
USING DELTA
PARTITIONED BY (processing_date DATE)
LOCATION 'dbfs:/user/hive/warehouse/gm_test_schema.db/silver_gm_data_2023'
COMMENT 'Silver layer - Cleansed and deduplicated GM data for 2023. Data quality validated.'
TBLPROPERTIES (
  'delta.enableChangeDataFeed' = 'true',
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact' = 'true',
  'delta.deletedFileRetentionDuration' = 'interval 7 days',
  'delta.logRetentionDuration' = 'interval 30 days',
  'quality_tier' = 'silver',
  'data_classification' = 'cleansed',
  'owner' = 'data_engineering'
);

-- Add constraints for data quality
-- ALTER TABLE gm_test_schema.silver_gm_data_2023 
-- ADD CONSTRAINT valid_records CHECK (_is_valid = true);

-- Create indexes for common queries (if supported)
-- CREATE INDEX idx_processing_date ON gm_test_schema.silver_gm_data_2023 (processing_date);
