"""
Pipeline Verification and Execution Helper

This script helps verify the configuration and execute the pipeline.
"""

import json
import os
import sys
from pathlib import Path


def print_header(title):
    """Print a formatted header"""
    print("\n" + "=" * 80)
    print(f"  {title}")
    print("=" * 80 + "\n")


def verify_configuration():
    """Verify that configuration file exists and is valid"""
    print_header("Configuration Verification")
    
    config_path = "config/data_config.json"
    
    if not os.path.exists(config_path):
        print(f"❌ Configuration file not found: {config_path}")
        print("\nPlease run: python run_discovery.py --sample")
        return False
    
    try:
        with open(config_path, 'r') as f:
            config = json.load(f)
        
        print(f"✅ Configuration file found and valid: {config_path}")
        print(f"\nConfiguration Summary:")
        print(f"  • Key Columns: {len(config.get('key_columns', []))}")
        print(f"  • Date Columns: {len(config.get('date_columns', []))}")
        print(f"  • Metric Columns: {len(config.get('metric_columns', []))}")
        print(f"  • Dimension Columns: {len(config.get('dimension_columns', []))}")
        print(f"  • Total Columns: {len(config.get('all_columns', []))}")
        
        print("\nKey Configuration:")
        print(f"  • Primary Key: {config.get('key_columns', ['N/A'])[0]}")
        print(f"  • Date Column: {config.get('date_columns', ['N/A'])[0]}")
        print(f"  • Main Metrics: {', '.join(config.get('metric_columns', [])[:3])}")
        
        return True
        
    except json.JSONDecodeError as e:
        print(f"❌ Invalid JSON in configuration file: {e}")
        return False
    except Exception as e:
        print(f"❌ Error reading configuration: {e}")
        return False


def verify_structure():
    """Verify project structure"""
    print_header("Project Structure Verification")
    
    required_paths = {
        "directories": [
            "src/gm_analysis/bronze",
            "src/gm_analysis/silver",
            "src/gm_analysis/gold",
            "src/gm_analysis/analytics",
            "src/gm_analysis/sql",
            "notebooks",
            "config",
            "resources"
        ],
        "files": [
            "run_pipeline.py",
            "src/gm_analysis/bronze/ingest_data.py",
            "src/gm_analysis/silver/cleanse_data.py",
            "src/gm_analysis/gold/aggregate_metrics.py",
            "src/gm_analysis/sql/validate_delta.py",
            "src/gm_analysis/sql/optimize.py",
            "src/gm_analysis/analytics/visualize_metrics.py"
        ]
    }
    
    all_exist = True
    
    # Check directories
    print("Directories:")
    for dir_path in required_paths["directories"]:
        exists = os.path.isdir(dir_path)
        status = "✅" if exists else "❌"
        print(f"  {status} {dir_path}")
        all_exist = all_exist and exists
    
    # Check files
    print("\nKey Files:")
    for file_path in required_paths["files"]:
        exists = os.path.isfile(file_path)
        status = "✅" if exists else "❌"
        print(f"  {status} {file_path}")
        all_exist = all_exist and exists
    
    if all_exist:
        print("\n✅ All required files and directories exist")
    else:
        print("\n❌ Some files or directories are missing")
    
    return all_exist


def print_usage_examples():
    """Print usage examples"""
    print_header("Pipeline Execution Examples")
    
    examples = [
        ("Full Pipeline", "python run_pipeline.py --year 2023 --config config/data_config.json"),
        ("Bronze Layer Only", "python run_pipeline.py --year 2023 --step bronze --config config/data_config.json"),
        ("Silver Layer Only", "python run_pipeline.py --year 2023 --step silver --config config/data_config.json"),
        ("Gold Layer Only", "python run_pipeline.py --year 2023 --step gold --config config/data_config.json"),
        ("With Optimization", "python run_pipeline.py --year 2023 --step optimization --vacuum --config config/data_config.json"),
        ("Skip Bronze", "python run_pipeline.py --year 2023 --skip-bronze --config config/data_config.json"),
        ("Skip Validation", "python run_pipeline.py --year 2023 --skip-validation --config config/data_config.json")
    ]
    
    for title, command in examples:
        print(f"\n{title}:")
        print(f"  {command}")


def print_databricks_commands():
    """Print Databricks-related commands"""
    print_header("Databricks Commands")
    
    commands = [
        ("Check Auth Status", "databricks auth profiles"),
        ("View Discovery Notebook", "databricks workspace export --profile CJ_External /Shared/gm_analysis/01_data_discovery --format SOURCE"),
        ("Re-run Discovery", "databricks jobs run-now --profile CJ_External 964101530962199"),
        ("Check Volume", "databricks fs ls --profile CJ_External dbfs:/Volumes/gm_demo/gm_test_schema/gm_volume/2023"),
    ]
    
    for title, command in commands:
        print(f"\n{title}:")
        print(f"  {command}")


def main():
    """Main function"""
    print("\n" + "▓" * 80)
    print("  GM Data Analysis - Pipeline Verification & Helper")
    print("▓" * 80)
    
    # Run verifications
    config_ok = verify_configuration()
    structure_ok = verify_structure()
    
    # Print usage
    if config_ok and structure_ok:
        print_header("Status: ✅ READY TO RUN")
        print("All checks passed! Your pipeline is ready to execute.")
        print_usage_examples()
        print_databricks_commands()
        
        print_header("Recommended Next Steps")
        print("1. Review configuration: cat config/data_config.json")
        print("2. Start with Bronze layer: python run_pipeline.py --year 2023 --step bronze --config config/data_config.json")
        print("3. Continue with Silver: python run_pipeline.py --year 2023 --step silver --config config/data_config.json")
        print("4. Complete with Gold: python run_pipeline.py --year 2023 --step gold --config config/data_config.json")
        print("5. Run validation: python run_pipeline.py --year 2023 --step validation --config config/data_config.json")
        print("6. Optimize tables: python run_pipeline.py --year 2023 --step optimization --vacuum --config config/data_config.json")
        
        print("\n" + "▓" * 80)
        print("  For full documentation, see: DISCOVERY_COMPLETE.md")
        print("▓" * 80 + "\n")
        
    else:
        print_header("Status: ❌ ISSUES FOUND")
        print("Please resolve the issues above before running the pipeline.")
        
        if not config_ok:
            print("\n❌ Configuration issue: Run 'python run_discovery.py --sample' to create config")
        
        if not structure_ok:
            print("\n❌ Structure issue: Some required files/directories are missing")
        
        print("\n" + "▓" * 80 + "\n")


if __name__ == "__main__":
    main()
