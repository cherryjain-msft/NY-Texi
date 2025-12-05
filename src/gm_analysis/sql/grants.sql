-- Unity Catalog Access Control Grants
-- Run these commands to set up permissions

-- ============================================================================
-- CATALOG LEVEL GRANTS
-- ============================================================================

-- Grant data engineering team full access to catalog
GRANT USE CATALOG ON CATALOG gm_demo TO `data_engineering`;
GRANT CREATE SCHEMA ON CATALOG gm_demo TO `data_engineering`;

-- Grant analytics team read access to catalog
GRANT USE CATALOG ON CATALOG gm_demo TO `analytics_team`;

-- ============================================================================
-- SCHEMA LEVEL GRANTS
-- ============================================================================

-- Grant data engineering team schema permissions
GRANT USE SCHEMA ON SCHEMA gm_demo.gm_test_schema TO `data_engineering`;
GRANT CREATE TABLE ON SCHEMA gm_demo.gm_test_schema TO `data_engineering`;
GRANT MODIFY ON SCHEMA gm_demo.gm_test_schema TO `data_engineering`;

-- Grant analytics team read access to schema
GRANT USE SCHEMA ON SCHEMA gm_demo.gm_test_schema TO `analytics_team`;

-- ============================================================================
-- TABLE LEVEL GRANTS - BRONZE LAYER
-- ============================================================================

-- Bronze tables - Data Engineering only
GRANT SELECT, MODIFY ON TABLE gm_demo.gm_test_schema.bronze_gm_data_2023 TO `data_engineering`;

-- ============================================================================
-- TABLE LEVEL GRANTS - SILVER LAYER
-- ============================================================================

-- Silver tables - Data Engineering (full access) and Analytics (read)
GRANT SELECT, MODIFY ON TABLE gm_demo.gm_test_schema.silver_gm_data_2023 TO `data_engineering`;
GRANT SELECT ON TABLE gm_demo.gm_test_schema.silver_gm_data_2023 TO `analytics_team`;

-- ============================================================================
-- TABLE LEVEL GRANTS - GOLD LAYER
-- ============================================================================

-- Gold tables - Data Engineering (full), Analytics (read), Business Users (read)
GRANT SELECT, MODIFY ON TABLE gm_demo.gm_test_schema.gold_gm_data_2023_daily TO `data_engineering`;
GRANT SELECT ON TABLE gm_demo.gm_test_schema.gold_gm_data_2023_daily TO `analytics_team`;
GRANT SELECT ON TABLE gm_demo.gm_test_schema.gold_gm_data_2023_daily TO `business_users`;

GRANT SELECT, MODIFY ON TABLE gm_demo.gm_test_schema.gold_gm_data_2023_monthly TO `data_engineering`;
GRANT SELECT ON TABLE gm_demo.gm_test_schema.gold_gm_data_2023_monthly TO `analytics_team`;
GRANT SELECT ON TABLE gm_demo.gm_test_schema.gold_gm_data_2023_monthly TO `business_users`;

GRANT SELECT, MODIFY ON TABLE gm_demo.gm_test_schema.gold_gm_data_2023_dimensions TO `data_engineering`;
GRANT SELECT ON TABLE gm_demo.gm_test_schema.gold_gm_data_2023_dimensions TO `analytics_team`;
GRANT SELECT ON TABLE gm_demo.gm_test_schema.gold_gm_data_2023_dimensions TO `business_users`;

-- ============================================================================
-- VOLUME GRANTS
-- ============================================================================

-- Grant access to volume
GRANT READ VOLUME ON VOLUME gm_demo.gm_test_schema.gm_volume TO `data_engineering`;
GRANT WRITE VOLUME ON VOLUME gm_demo.gm_test_schema.gm_volume TO `data_engineering`;

-- ============================================================================
-- ROW LEVEL SECURITY (Example - Uncomment and customize as needed)
-- ============================================================================

-- Example: Restrict access to specific regions
-- CREATE FUNCTION gm_demo.gm_test_schema.regional_filter(region STRING)
-- RETURN IF(
--   IS_MEMBER('regional_managers'),
--   region IN (SELECT region FROM authorized_regions WHERE user = CURRENT_USER()),
--   TRUE
-- );

-- Apply row filter
-- ALTER TABLE gm_demo.gm_test_schema.silver_gm_data_2023
-- SET ROW FILTER gm_demo.gm_test_schema.regional_filter ON (region);

-- ============================================================================
-- COLUMN LEVEL SECURITY (Example - Uncomment and customize as needed)
-- ============================================================================

-- Example: Mask sensitive columns
-- CREATE FUNCTION gm_demo.gm_test_schema.mask_pii(value STRING)
-- RETURN IF(
--   IS_MEMBER('pii_access'),
--   value,
--   'REDACTED'
-- );

-- Apply column mask
-- ALTER TABLE gm_demo.gm_test_schema.silver_gm_data_2023
-- ALTER COLUMN customer_name SET MASK gm_demo.gm_test_schema.mask_pii;

-- ============================================================================
-- TABLE TAGS AND METADATA
-- ============================================================================

-- Tag bronze tables
ALTER TABLE gm_demo.gm_test_schema.bronze_gm_data_2023 
SET TAGS ('layer' = 'bronze', 'data_classification' = 'raw', 'pii_data' = 'false');

-- Tag silver tables
ALTER TABLE gm_demo.gm_test_schema.silver_gm_data_2023 
SET TAGS ('layer' = 'silver', 'data_classification' = 'cleansed', 'pii_data' = 'false');

-- Tag gold tables
ALTER TABLE gm_demo.gm_test_schema.gold_gm_data_2023_daily 
SET TAGS ('layer' = 'gold', 'data_classification' = 'aggregated', 'business_critical' = 'true');

ALTER TABLE gm_demo.gm_test_schema.gold_gm_data_2023_monthly 
SET TAGS ('layer' = 'gold', 'data_classification' = 'aggregated', 'business_critical' = 'true');

ALTER TABLE gm_demo.gm_test_schema.gold_gm_data_2023_dimensions 
SET TAGS ('layer' = 'gold', 'data_classification' = 'dimension', 'business_critical' = 'true');

-- ============================================================================
-- VERIFICATION QUERIES
-- ============================================================================

-- Show all grants for a specific table
-- SHOW GRANTS ON TABLE gm_demo.gm_test_schema.gold_gm_data_2023_daily;

-- Show current user's grants
-- SHOW GRANTS ON CATALOG gm_demo;
-- SHOW GRANTS ON SCHEMA gm_demo.gm_test_schema;

-- Show table tags
-- DESCRIBE TABLE EXTENDED gm_demo.gm_test_schema.gold_gm_data_2023_daily;
