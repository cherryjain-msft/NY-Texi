"""
Data Discovery Runner

This script executes the data discovery notebook in Databricks and
saves the configuration to a JSON file.

Usage:
    # In Databricks:
    dbx execute --notebook notebooks/01_data_discovery.py
    
    # Or using databricks CLI:
    databricks workspace import notebooks/01_data_discovery.py --language PYTHON --format SOURCE
    databricks workspace export notebooks/01_data_discovery.py
"""

import json
import logging
from pathlib import Path

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def create_config_from_discovery(schema_info: dict, output_path: str = "config/data_config.json"):
    """
    Create configuration file from data discovery results.
    
    Args:
        schema_info: Dictionary containing column names and types from discovery
        output_path: Path to save configuration JSON
    """
    
    # Extract columns by type
    key_columns = []
    date_columns = []
    metric_columns = []
    dimension_columns = []
    
    for col_name, col_type in schema_info.items():
        col_type_lower = col_type.lower()
        col_name_lower = col_name.lower()
        
        # Identify key columns (IDs, primary keys)
        if 'id' in col_name_lower or 'key' in col_name_lower:
            key_columns.append(col_name)
        
        # Identify date/timestamp columns
        elif 'timestamp' in col_type_lower or 'date' in col_type_lower:
            date_columns.append(col_name)
        elif 'time' in col_name_lower or 'date' in col_name_lower:
            date_columns.append(col_name)
        
        # Identify numeric metric columns
        elif col_type_lower in ['int', 'bigint', 'long', 'double', 'float', 'decimal']:
            if 'amount' in col_name_lower or 'fare' in col_name_lower or \
               'tip' in col_name_lower or 'total' in col_name_lower or \
               'distance' in col_name_lower or 'count' in col_name_lower:
                metric_columns.append(col_name)
            else:
                dimension_columns.append(col_name)
        
        # String columns are usually dimensions
        elif col_type_lower in ['string', 'varchar']:
            dimension_columns.append(col_name)
    
    config = {
        "key_columns": key_columns,
        "date_columns": date_columns,
        "metric_columns": metric_columns,
        "dimension_columns": dimension_columns,
        "all_columns": list(schema_info.keys()),
        "schema_info": schema_info
    }
    
    # Save configuration
    output_file = Path(output_path)
    output_file.parent.mkdir(parents=True, exist_ok=True)
    
    with open(output_file, 'w') as f:
        json.dump(config, f, indent=2)
    
    logger.info(f"Configuration saved to {output_path}")
    logger.info(f"  Key columns: {len(key_columns)}")
    logger.info(f"  Date columns: {len(date_columns)}")
    logger.info(f"  Metric columns: {len(metric_columns)}")
    logger.info(f"  Dimension columns: {len(dimension_columns)}")
    
    return config


def print_discovery_instructions():
    """Print instructions for running discovery in Databricks."""
    
    instructions = """
╔════════════════════════════════════════════════════════════════════════════╗
║                    DATA DISCOVERY INSTRUCTIONS                             ║
╚════════════════════════════════════════════════════════════════════════════╝

To discover your data schema and update the pipeline configuration:

OPTION 1: Run in Databricks Workspace
──────────────────────────────────────

1. Upload the discovery notebook:
   
   databricks workspace import \\
     ./notebooks/01_data_discovery.py \\
     /Workspace/Users/<your-email>/gm_analysis/01_data_discovery \\
     --language PYTHON

2. Open in Databricks UI and run all cells

3. Copy the schema information from the output

4. Run this script with the schema info:
   
   python run_discovery.py


OPTION 2: Use Databricks CLI
─────────────────────────────

1. Check if databricks CLI is installed:
   
   databricks --version

2. Configure authentication:
   
   databricks configure --token

3. Run the notebook:
   
   databricks workspace import notebooks/01_data_discovery.py \\
     --language PYTHON --format SOURCE

4. View results in Databricks workspace


OPTION 3: Manual Schema Discovery
──────────────────────────────────

Run these SQL commands in Databricks SQL Editor:

-- List files in volume
LIST '/Volumes/gm_demo/gm_test_schema/gm_volume/2023/';

-- Infer schema from sample file (update path)
CREATE OR REPLACE TEMP VIEW temp_sample AS
SELECT * FROM parquet.`/Volumes/gm_demo/gm_test_schema/gm_volume/2023/*.parquet`
LIMIT 10;

-- Get schema
DESCRIBE temp_sample;

-- Get column statistics
DESCRIBE EXTENDED temp_sample;


OPTION 4: Use Sample Configuration
───────────────────────────────────

If your data matches taxi/ride-sharing patterns, use the sample config:

   python run_pipeline.py --year 2023 \\
     --config config/data_discovery_sample.json

The sample includes common columns:
  • Key: trip_id, vendor_id
  • Dates: pickup_datetime, dropoff_datetime
  • Metrics: fare_amount, tip_amount, total_amount, trip_distance
  • Dimensions: passenger_count, payment_type, rate_code


NEXT STEPS
──────────

After discovery, run the pipeline:

   python run_pipeline.py --year 2023 --config config/data_config.json

Or run specific steps:

   python run_pipeline.py --year 2023 --step bronze
   python run_pipeline.py --year 2023 --step silver
   python run_pipeline.py --year 2023 --step gold

╚════════════════════════════════════════════════════════════════════════════╝
"""
    
    print(instructions)


if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1 and sys.argv[1] == "--help":
        print_discovery_instructions()
    elif len(sys.argv) > 1 and sys.argv[1] == "--sample":
        # Create sample schema for demonstration
        sample_schema = {
            "trip_id": "string",
            "vendor_id": "int",
            "pickup_datetime": "timestamp",
            "dropoff_datetime": "timestamp",
            "passenger_count": "int",
            "trip_distance": "double",
            "pickup_longitude": "double",
            "pickup_latitude": "double",
            "rate_code": "int",
            "store_and_fwd_flag": "string",
            "dropoff_longitude": "double",
            "dropoff_latitude": "double",
            "payment_type": "int",
            "fare_amount": "double",
            "extra": "double",
            "mta_tax": "double",
            "tip_amount": "double",
            "tolls_amount": "double",
            "total_amount": "double"
        }
        
        config = create_config_from_discovery(sample_schema, "config/data_config.json")
        print("\n✓ Sample configuration created at config/data_config.json")
        print(json.dumps(config, indent=2))
    else:
        print_discovery_instructions()
