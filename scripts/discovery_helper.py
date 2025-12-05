"""
Local Data Discovery Script
This script helps you understand your data structure to configure the pipeline.

Since we can't directly access your Databricks volume, this script provides:
1. A template for documenting your data structure
2. Instructions for running discovery in Databricks
3. A configuration generator based on your findings
"""

import json
from datetime import datetime

class DataDiscoveryHelper:
    """Helper class to collect and document data structure"""
    
    def __init__(self):
        self.discovery = {
            "discovery_date": datetime.now().isoformat(),
            "year": "2023",
            "volume_path": "/Volumes/gm_demo/gm_test_schema/gm_volume/2023",
            "files": [],
            "schemas": [],
            "key_columns": [],
            "date_columns": [],
            "metric_columns": [],
            "dimension_columns": []
        }
    
    def collect_manual_input(self):
        """Collect data structure information interactively"""
        print("=" * 80)
        print("GM DATA DISCOVERY - CONFIGURATION HELPER")
        print("=" * 80)
        print("\nPlease run this SQL in Databricks to explore your volume:")
        print(f"\nSELECT * FROM dbutils.fs.ls('{self.discovery['volume_path']}') LIMIT 10;")
        print("\nOr use the notebook: notebooks/01_data_discovery.py")
        print("\n" + "=" * 80)
        
        # File format
        print("\n1. What file format did you find? (parquet/csv/json)")
        file_format = input("   Format: ").strip().lower() or "parquet"
        self.discovery["file_format"] = file_format
        
        # Sample table
        print("\n2. Run this to see a sample of your data:")
        if file_format == "parquet":
            print(f"   df = spark.read.parquet('{self.discovery['volume_path']}/*.parquet')")
        elif file_format == "csv":
            print(f"   df = spark.read.option('header', 'true').csv('{self.discovery['volume_path']}/*.csv')")
        else:
            print(f"   df = spark.read.json('{self.discovery['volume_path']}/*.json')")
        print("   df.printSchema()")
        print("   df.show(5)")
        
        input("\n   Press Enter after you've reviewed the schema...")
        
        # Key columns
        print("\n3. Which column(s) uniquely identify each record?")
        print("   (comma-separated, e.g., 'id' or 'transaction_id,customer_id')")
        key_cols = input("   Key columns: ").strip()
        self.discovery["key_columns"] = [c.strip() for c in key_cols.split(",") if c.strip()]
        
        # Date columns
        print("\n4. Which column contains the date/timestamp for partitioning?")
        print("   (e.g., 'transaction_date', 'created_at', 'event_timestamp')")
        date_col = input("   Date column: ").strip()
        self.discovery["date_columns"] = [date_col] if date_col else []
        
        # Metric columns
        print("\n5. Which columns contain numeric values to aggregate?")
        print("   (comma-separated, e.g., 'amount,quantity,price')")
        metric_cols = input("   Metric columns: ").strip()
        self.discovery["metric_columns"] = [c.strip() for c in metric_cols.split(",") if c.strip()]
        
        # Dimension columns
        print("\n6. Which columns are categorical (for grouping/filtering)?")
        print("   (comma-separated, e.g., 'category,region,product_type')")
        dim_cols = input("   Dimension columns: ").strip()
        self.discovery["dimension_columns"] = [c.strip() for c in dim_cols.split(",") if c.strip()]
        
        return self.discovery
    
    def generate_config(self):
        """Generate configuration code based on discovery"""
        print("\n" + "=" * 80)
        print("GENERATED CONFIGURATION")
        print("=" * 80)
        
        # For run_pipeline.py
        print("\n### Update run_pipeline.py ###")
        print("\n# Lines 75-80 (Silver Layer):")
        print(f"""
key_columns={self.discovery['key_columns']},
string_columns=None,  # Add your string columns if needed
partition_cols=["{self.discovery['date_columns'][0]}"] if self.discovery['date_columns'] else ["processing_date"]
        """.strip())
        
        print("\n# Lines 105-109 (Gold Layer):")
        date_col = self.discovery['date_columns'][0] if self.discovery['date_columns'] else "transaction_date"
        print(f"""
date_column="{date_col}",
metric_columns={self.discovery['metric_columns']},
dimension_columns={self.discovery['dimension_columns']}
        """.strip())
        
        # Save to file
        config_file = "data_discovery_config.json"
        with open(config_file, 'w') as f:
            json.dump(self.discovery, f, indent=2)
        
        print(f"\n\nConfiguration saved to: {config_file}")
        print("\nNext steps:")
        print("1. Review the generated configuration above")
        print("2. Update run_pipeline.py with these values")
        print("3. Run: python run_pipeline.py --year 2023")
        
        return self.discovery
    
    def save_discovery(self, filepath="data_discovery_config.json"):
        """Save discovery to JSON file"""
        with open(filepath, 'w') as f:
            json.dump(self.discovery, f, indent=2)
        print(f"\nDiscovery saved to: {filepath}")


def main():
    """Main function"""
    helper = DataDiscoveryHelper()
    
    print("\n" + "=" * 80)
    print("This helper will guide you through documenting your data structure")
    print("=" * 80)
    
    choice = input("\nWould you like to enter data structure manually? (y/n): ").strip().lower()
    
    if choice == 'y':
        discovery = helper.collect_manual_input()
        helper.generate_config()
    else:
        print("\nPlease run the Databricks notebook: notebooks/01_data_discovery.py")
        print("Then come back and run this script with 'y' to configure.")


if __name__ == "__main__":
    main()
