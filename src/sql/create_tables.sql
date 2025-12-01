-- Create Delta tables for NYC Taxi data in Unity Catalog
-- Raw layer (Bronze) - stores ingested parquet data

-- ============================================
-- 1. TAXI ZONES REFERENCE TABLE
-- ============================================
CREATE TABLE IF NOT EXISTS ${catalog}.raw.taxi_zones (
  LocationID INT COMMENT 'Unique identifier for taxi zone',
  Borough STRING COMMENT 'NYC borough name',
  Zone STRING COMMENT 'Taxi zone name',
  service_zone STRING COMMENT 'Type of service zone (Yellow Zone, Boro Zone, EWR)'
)
USING DELTA
COMMENT 'NYC Taxi Zone lookup table - reference data for location IDs'
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact' = 'true',
  'delta.logRetentionDuration' = '30 days',
  'delta.deletedFileRetentionDuration' = '7 days'
);

-- ============================================
-- 2. YELLOW TAXI TRIPS TABLE
-- ============================================
CREATE TABLE IF NOT EXISTS ${catalog}.raw.yellow_taxi_trips (
  VendorID INT COMMENT 'Taxi vendor ID (1=Creative Mobile, 2=VeriFone)',
  tpep_pickup_datetime TIMESTAMP COMMENT 'Pickup date and time',
  tpep_dropoff_datetime TIMESTAMP COMMENT 'Dropoff date and time',
  passenger_count DOUBLE COMMENT 'Number of passengers',
  trip_distance DOUBLE COMMENT 'Trip distance in miles',
  RatecodeID DOUBLE COMMENT 'Rate code (1=Standard, 2=JFK, 3=Newark, etc)',
  store_and_fwd_flag STRING COMMENT 'Store and forward flag (Y/N)',
  PULocationID INT COMMENT 'Pickup location ID',
  DOLocationID INT COMMENT 'Dropoff location ID',
  payment_type INT COMMENT 'Payment type (1=Credit, 2=Cash, 3=No Charge, etc)',
  fare_amount DOUBLE COMMENT 'Fare amount in dollars',
  extra DOUBLE COMMENT 'Extra charges',
  mta_tax DOUBLE COMMENT 'MTA tax',
  tip_amount DOUBLE COMMENT 'Tip amount',
  tolls_amount DOUBLE COMMENT 'Tolls amount',
  improvement_surcharge DOUBLE COMMENT 'Improvement surcharge',
  total_amount DOUBLE COMMENT 'Total amount charged',
  congestion_surcharge DOUBLE COMMENT 'Congestion surcharge',
  airport_fee DOUBLE COMMENT 'Airport fee',
  -- Partition columns
  year INT COMMENT 'Year of pickup',
  month INT COMMENT 'Month of pickup'
)
USING DELTA
PARTITIONED BY (year, month)
COMMENT 'Yellow taxi trip records - Manhattan and airports'
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact' = 'true',
  'delta.logRetentionDuration' = '90 days',
  'delta.deletedFileRetentionDuration' = '7 days',
  'delta.checkpoint.writeStatsAsJson' = 'false',
  'delta.checkpoint.writeStatsAsStruct' = 'true'
);

-- ============================================
-- 3. GREEN TAXI TRIPS TABLE
-- ============================================
CREATE TABLE IF NOT EXISTS ${catalog}.raw.green_taxi_trips (
  VendorID INT COMMENT 'Taxi vendor ID',
  lpep_pickup_datetime TIMESTAMP COMMENT 'Pickup date and time',
  lpep_dropoff_datetime TIMESTAMP COMMENT 'Dropoff date and time',
  passenger_count DOUBLE COMMENT 'Number of passengers',
  trip_distance DOUBLE COMMENT 'Trip distance in miles',
  RatecodeID DOUBLE COMMENT 'Rate code',
  store_and_fwd_flag STRING COMMENT 'Store and forward flag (Y/N)',
  PULocationID INT COMMENT 'Pickup location ID',
  DOLocationID INT COMMENT 'Dropoff location ID',
  payment_type INT COMMENT 'Payment type',
  fare_amount DOUBLE COMMENT 'Fare amount in dollars',
  extra DOUBLE COMMENT 'Extra charges',
  mta_tax DOUBLE COMMENT 'MTA tax',
  tip_amount DOUBLE COMMENT 'Tip amount',
  tolls_amount DOUBLE COMMENT 'Tolls amount',
  improvement_surcharge DOUBLE COMMENT 'Improvement surcharge',
  total_amount DOUBLE COMMENT 'Total amount charged',
  congestion_surcharge DOUBLE COMMENT 'Congestion surcharge',
  trip_type INT COMMENT 'Trip type (1=Street-hail, 2=Dispatch)',
  ehail_fee DOUBLE COMMENT 'E-hail fee',
  -- Partition columns
  year INT COMMENT 'Year of pickup',
  month INT COMMENT 'Month of pickup'
)
USING DELTA
PARTITIONED BY (year, month)
COMMENT 'Green taxi trip records - outer boroughs'
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact' = 'true',
  'delta.logRetentionDuration' = '90 days',
  'delta.deletedFileRetentionDuration' = '7 days',
  'delta.checkpoint.writeStatsAsJson' = 'false',
  'delta.checkpoint.writeStatsAsStruct' = 'true'
);

-- ============================================
-- 4. FOR-HIRE VEHICLE (FHV) TRIPS TABLE
-- ============================================
CREATE TABLE IF NOT EXISTS ${catalog}.raw.fhv_trips (
  dispatching_base_num STRING COMMENT 'TLC base license number',
  pickup_datetime TIMESTAMP COMMENT 'Pickup date and time',
  dropOff_datetime TIMESTAMP COMMENT 'Dropoff date and time',
  PUlocationID INT COMMENT 'Pickup location ID',
  DOlocationID INT COMMENT 'Dropoff location ID',
  SR_Flag INT COMMENT 'Shared ride flag',
  Affiliated_base_number STRING COMMENT 'Affiliated base number',
  -- Partition columns
  year INT COMMENT 'Year of pickup',
  month INT COMMENT 'Month of pickup'
)
USING DELTA
PARTITIONED BY (year, month)
COMMENT 'For-hire vehicle trip records - black cars, livery'
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact' = 'true',
  'delta.logRetentionDuration' = '90 days',
  'delta.deletedFileRetentionDuration' = '7 days',
  'delta.checkpoint.writeStatsAsJson' = 'false',
  'delta.checkpoint.writeStatsAsStruct' = 'true'
);

-- ============================================
-- 5. HIGH-VOLUME FHV (HVFHS) TRIPS TABLE
-- ============================================
CREATE TABLE IF NOT EXISTS ${catalog}.raw.hvfhs_trips (
  hvfhs_license_num STRING COMMENT 'HVFHS license number',
  dispatching_base_num STRING COMMENT 'TLC base license number',
  originating_base_num STRING COMMENT 'Originating base number',
  request_datetime TIMESTAMP COMMENT 'Request date and time',
  on_scene_datetime TIMESTAMP COMMENT 'On scene date and time',
  pickup_datetime TIMESTAMP COMMENT 'Pickup date and time',
  dropoff_datetime TIMESTAMP COMMENT 'Dropoff date and time',
  PULocationID INT COMMENT 'Pickup location ID',
  DOLocationID INT COMMENT 'Dropoff location ID',
  trip_miles DOUBLE COMMENT 'Trip distance in miles',
  trip_time LONG COMMENT 'Trip time in seconds',
  base_passenger_fare DOUBLE COMMENT 'Base passenger fare',
  tolls DOUBLE COMMENT 'Tolls amount',
  bcf DOUBLE COMMENT 'Black car fund',
  sales_tax DOUBLE COMMENT 'Sales tax',
  congestion_surcharge DOUBLE COMMENT 'Congestion surcharge',
  airport_fee DOUBLE COMMENT 'Airport fee',
  tips DOUBLE COMMENT 'Tips amount',
  driver_pay DOUBLE COMMENT 'Driver pay',
  shared_request_flag STRING COMMENT 'Shared request flag (Y/N)',
  shared_match_flag STRING COMMENT 'Shared match flag (Y/N)',
  access_a_ride_flag STRING COMMENT 'Access-a-ride flag (Y/N)',
  wav_request_flag STRING COMMENT 'Wheelchair accessible vehicle flag (Y/N)',
  wav_match_flag STRING COMMENT 'WAV match flag (Y/N)',
  -- Partition columns
  year INT COMMENT 'Year of pickup',
  month INT COMMENT 'Month of pickup'
)
USING DELTA
PARTITIONED BY (year, month)
COMMENT 'High-volume for-hire vehicle trips - Uber, Lyft, Via, etc.'
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact' = 'true',
  'delta.logRetentionDuration' = '90 days',
  'delta.deletedFileRetentionDuration' = '7 days',
  'delta.checkpoint.writeStatsAsJson' = 'false',
  'delta.checkpoint.writeStatsAsStruct' = 'true'
);
