-- Create Curated Layer (Silver) Tables
-- Cleaned, validated, and enriched data with business logic applied

-- ============================================
-- 1. CURATED YELLOW TAXI TRIPS
-- ============================================
CREATE OR REPLACE TABLE ${catalog}.curated.yellow_taxi_trips AS
SELECT 
  -- Trip identifiers
  CONCAT(
    CAST(year AS STRING), '-',
    LPAD(CAST(month AS STRING), 2, '0'), '-',
    CAST(VendorID AS STRING), '-',
    CAST(UNIX_TIMESTAMP(tpep_pickup_datetime) AS STRING)
  ) AS trip_id,
  
  -- Vendor and metadata
  VendorID,
  CASE 
    WHEN VendorID = 1 THEN 'Creative Mobile Technologies'
    WHEN VendorID = 2 THEN 'VeriFone Inc'
    ELSE 'Unknown'
  END AS vendor_name,
  
  -- Datetime fields
  tpep_pickup_datetime,
  tpep_dropoff_datetime,
  TIMESTAMPDIFF(MINUTE, tpep_pickup_datetime, tpep_dropoff_datetime) AS trip_duration_minutes,
  HOUR(tpep_pickup_datetime) AS pickup_hour,
  DAYOFWEEK(tpep_pickup_datetime) AS pickup_day_of_week,
  CASE 
    WHEN DAYOFWEEK(tpep_pickup_datetime) IN (1, 7) THEN 'Weekend'
    ELSE 'Weekday'
  END AS day_type,
  
  -- Trip details
  passenger_count,
  trip_distance,
  RatecodeID,
  CASE 
    WHEN RatecodeID = 1 THEN 'Standard rate'
    WHEN RatecodeID = 2 THEN 'JFK'
    WHEN RatecodeID = 3 THEN 'Newark'
    WHEN RatecodeID = 4 THEN 'Nassau or Westchester'
    WHEN RatecodeID = 5 THEN 'Negotiated fare'
    WHEN RatecodeID = 6 THEN 'Group ride'
    ELSE 'Unknown'
  END AS rate_code_desc,
  store_and_fwd_flag,
  
  -- Location with zone names
  raw.PULocationID,
  pu_zone.Zone AS pickup_zone,
  pu_zone.Borough AS pickup_borough,
  pu_zone.service_zone AS pickup_service_zone,
  
  raw.DOLocationID,
  do_zone.Zone AS dropoff_zone,
  do_zone.Borough AS dropoff_borough,
  do_zone.service_zone AS dropoff_service_zone,
  
  -- Payment information
  payment_type,
  CASE 
    WHEN payment_type = 1 THEN 'Credit card'
    WHEN payment_type = 2 THEN 'Cash'
    WHEN payment_type = 3 THEN 'No charge'
    WHEN payment_type = 4 THEN 'Dispute'
    WHEN payment_type = 5 THEN 'Unknown'
    WHEN payment_type = 6 THEN 'Voided trip'
    ELSE 'Other'
  END AS payment_type_desc,
  
  -- Financial details
  fare_amount,
  extra,
  mta_tax,
  tip_amount,
  CASE 
    WHEN fare_amount > 0 THEN (tip_amount / fare_amount) * 100
    ELSE 0
  END AS tip_percentage,
  tolls_amount,
  improvement_surcharge,
  congestion_surcharge,
  airport_fee,
  total_amount,
  
  -- Data quality flags
  CASE 
    WHEN trip_distance < 0 OR fare_amount < 0 OR total_amount < 0 THEN TRUE
    ELSE FALSE
  END AS has_negative_values,
  
  CASE 
    WHEN tpep_dropoff_datetime <= tpep_pickup_datetime THEN TRUE
    ELSE FALSE
  END AS invalid_trip_duration,
  
  CASE 
    WHEN trip_distance > 0 AND TIMESTAMPDIFF(MINUTE, tpep_pickup_datetime, tpep_dropoff_datetime) > 0
    THEN trip_distance / (TIMESTAMPDIFF(MINUTE, tpep_pickup_datetime, tpep_dropoff_datetime) / 60.0)
    ELSE 0
  END AS avg_speed_mph,
  
  -- Partition columns
  year,
  month
  
FROM ${catalog}.raw.yellow_taxi_trips raw
LEFT JOIN ${catalog}.raw.taxi_zones pu_zone 
  ON raw.PULocationID = pu_zone.LocationID
LEFT JOIN ${catalog}.raw.taxi_zones do_zone 
  ON raw.DOLocationID = do_zone.LocationID
WHERE 
  -- Data quality filters
  tpep_pickup_datetime IS NOT NULL
  AND tpep_dropoff_datetime IS NOT NULL
  AND tpep_dropoff_datetime > tpep_pickup_datetime
  AND trip_distance >= 0
  AND fare_amount >= 0
  AND total_amount >= 0
  AND passenger_count > 0;

-- Add table properties
ALTER TABLE ${catalog}.curated.yellow_taxi_trips SET TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact' = 'true',
  'delta.logRetentionDuration' = '90 days',
  'delta.deletedFileRetentionDuration' = '7 days'
);

-- ============================================
-- 2. CURATED GREEN TAXI TRIPS
-- ============================================
CREATE OR REPLACE TABLE ${catalog}.curated.green_taxi_trips AS
SELECT 
  -- Trip identifiers
  CONCAT(
    CAST(year AS STRING), '-',
    LPAD(CAST(month AS STRING), 2, '0'), '-',
    CAST(VendorID AS STRING), '-',
    CAST(UNIX_TIMESTAMP(lpep_pickup_datetime) AS STRING)
  ) AS trip_id,
  
  -- Vendor and metadata
  VendorID,
  CASE 
    WHEN VendorID = 1 THEN 'Creative Mobile Technologies'
    WHEN VendorID = 2 THEN 'VeriFone Inc'
    ELSE 'Unknown'
  END AS vendor_name,
  
  -- Datetime fields
  lpep_pickup_datetime,
  lpep_dropoff_datetime,
  TIMESTAMPDIFF(MINUTE, lpep_pickup_datetime, lpep_dropoff_datetime) AS trip_duration_minutes,
  HOUR(lpep_pickup_datetime) AS pickup_hour,
  DAYOFWEEK(lpep_pickup_datetime) AS pickup_day_of_week,
  CASE 
    WHEN DAYOFWEEK(lpep_pickup_datetime) IN (1, 7) THEN 'Weekend'
    ELSE 'Weekday'
  END AS day_type,
  
  -- Trip details
  passenger_count,
  trip_distance,
  RatecodeID,
  store_and_fwd_flag,
  trip_type,
  CASE 
    WHEN trip_type = 1 THEN 'Street-hail'
    WHEN trip_type = 2 THEN 'Dispatch'
    ELSE 'Unknown'
  END AS trip_type_desc,
  
  -- Location with zone names
  raw.PULocationID,
  pu_zone.Zone AS pickup_zone,
  pu_zone.Borough AS pickup_borough,
  
  raw.DOLocationID,
  do_zone.Zone AS dropoff_zone,
  do_zone.Borough AS dropoff_borough,
  
  -- Payment information
  payment_type,
  
  -- Financial details
  fare_amount,
  extra,
  mta_tax,
  tip_amount,
  CASE 
    WHEN fare_amount > 0 THEN (tip_amount / fare_amount) * 100
    ELSE 0
  END AS tip_percentage,
  tolls_amount,
  improvement_surcharge,
  congestion_surcharge,
  ehail_fee,
  total_amount,
  
  -- Partition columns
  year,
  month
  
FROM ${catalog}.raw.green_taxi_trips raw
LEFT JOIN ${catalog}.raw.taxi_zones pu_zone 
  ON raw.PULocationID = pu_zone.LocationID
LEFT JOIN ${catalog}.raw.taxi_zones do_zone 
  ON raw.DOLocationID = do_zone.LocationID
WHERE 
  lpep_pickup_datetime IS NOT NULL
  AND lpep_dropoff_datetime IS NOT NULL
  AND lpep_dropoff_datetime > lpep_pickup_datetime
  AND trip_distance >= 0
  AND fare_amount >= 0
  AND total_amount >= 0;

-- Add table properties
ALTER TABLE ${catalog}.curated.green_taxi_trips SET TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact' = 'true',
  'delta.logRetentionDuration' = '90 days',
  'delta.deletedFileRetentionDuration' = '7 days'
);

-- ============================================
-- 3. ANALYTICS - MONTHLY TRIP SUMMARY
-- ============================================
CREATE OR REPLACE TABLE ${catalog}.analytics.monthly_trip_summary AS
SELECT 
  year,
  month,
  'Yellow Taxi' AS taxi_type,
  COUNT(*) AS total_trips,
  SUM(passenger_count) AS total_passengers,
  ROUND(AVG(trip_distance), 2) AS avg_trip_distance,
  ROUND(AVG(trip_duration_minutes), 2) AS avg_trip_duration_minutes,
  ROUND(AVG(fare_amount), 2) AS avg_fare_amount,
  ROUND(AVG(tip_percentage), 2) AS avg_tip_percentage,
  ROUND(SUM(total_amount), 2) AS total_revenue,
  ROUND(AVG(avg_speed_mph), 2) AS avg_speed_mph
FROM ${catalog}.curated.yellow_taxi_trips
WHERE NOT has_negative_values AND NOT invalid_trip_duration
GROUP BY year, month

UNION ALL

SELECT 
  year,
  month,
  'Green Taxi' AS taxi_type,
  COUNT(*) AS total_trips,
  SUM(passenger_count) AS total_passengers,
  ROUND(AVG(trip_distance), 2) AS avg_trip_distance,
  ROUND(AVG(trip_duration_minutes), 2) AS avg_trip_duration_minutes,
  ROUND(AVG(fare_amount), 2) AS avg_fare_amount,
  ROUND(AVG(tip_percentage), 2) AS avg_tip_percentage,
  ROUND(SUM(total_amount), 2) AS total_revenue,
  NULL AS avg_speed_mph
FROM ${catalog}.curated.green_taxi_trips
GROUP BY year, month

ORDER BY year DESC, month DESC, taxi_type;

-- Add table properties
ALTER TABLE ${catalog}.analytics.monthly_trip_summary SET TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact' = 'true'
);

-- ============================================
-- 4. ANALYTICS - POPULAR ROUTES
-- ============================================
CREATE OR REPLACE TABLE ${catalog}.analytics.popular_routes AS
SELECT 
  pickup_zone,
  pickup_borough,
  dropoff_zone,
  dropoff_borough,
  COUNT(*) AS trip_count,
  ROUND(AVG(trip_distance), 2) AS avg_distance,
  ROUND(AVG(fare_amount), 2) AS avg_fare,
  ROUND(AVG(trip_duration_minutes), 2) AS avg_duration_minutes,
  year,
  month
FROM ${catalog}.curated.yellow_taxi_trips
WHERE 
  pickup_zone IS NOT NULL 
  AND dropoff_zone IS NOT NULL
  AND NOT has_negative_values 
  AND NOT invalid_trip_duration
GROUP BY pickup_zone, pickup_borough, dropoff_zone, dropoff_borough, year, month
HAVING COUNT(*) >= 10
ORDER BY trip_count DESC;

-- Add table properties
ALTER TABLE ${catalog}.analytics.popular_routes SET TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact' = 'true'
);

-- ============================================
-- 5. ANALYTICS - HOURLY PATTERNS
-- ============================================
CREATE OR REPLACE TABLE ${catalog}.analytics.hourly_patterns AS
SELECT 
  pickup_hour,
  day_type,
  COUNT(*) AS trip_count,
  ROUND(AVG(trip_distance), 2) AS avg_distance,
  ROUND(AVG(fare_amount), 2) AS avg_fare,
  ROUND(AVG(tip_percentage), 2) AS avg_tip_percentage,
  year,
  month
FROM ${catalog}.curated.yellow_taxi_trips
WHERE NOT has_negative_values AND NOT invalid_trip_duration
GROUP BY pickup_hour, day_type, year, month
ORDER BY year DESC, month DESC, pickup_hour;

-- Add table properties
ALTER TABLE ${catalog}.analytics.hourly_patterns SET TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact' = 'true'
);
