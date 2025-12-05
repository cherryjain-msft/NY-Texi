"""
Main Orchestration Script for GM Medallion Architecture

This script coordinates the execution of the entire medallion pipeline:
Bronze → Silver → Gold → Validation → Optimization → Analytics

Usage:
    python run_pipeline.py --year 2023 [--skip-bronze] [--skip-validation]
"""

import argparse
import logging
import json
import os
from datetime import datetime
from pyspark.sql import SparkSession
from typing import Optional, Dict, Any

# Import pipeline modules
from src.gm_analysis.bronze.ingest_data import ingest_year_data
from src.gm_analysis.silver.cleanse_data import transform_bronze_to_silver
from src.gm_analysis.gold.aggregate_metrics import create_gold_aggregations
from src.gm_analysis.sql.validate_delta import run_full_validation_suite
from src.gm_analysis.sql.optimize import run_maintenance_job
from src.gm_analysis.analytics.visualize_metrics import create_visualizations

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f'pipeline_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class MedallionPipeline:
    """Orchestrate the complete medallion architecture pipeline"""
    
    def __init__(
        self,
        year: str,
        catalog: str = "gm_demo",
        schema: str = "gm_test_schema",
        config: Optional[Dict[str, Any]] = None
    ):
        self.year = year
        self.catalog = catalog
        self.schema = schema
        self.spark = SparkSession.builder.getOrCreate()
        self.config = config or {}
        
        logger.info(f"Pipeline initialized for year {year}")
        logger.info(f"Target: {catalog}.{schema}")
        if config:
            logger.info(f"Configuration loaded with {len(config)} keys")
    
    def run_bronze_layer(self) -> bool:
        """Execute Bronze layer ingestion"""
        try:
            logger.info("=" * 80)
            logger.info("STARTING BRONZE LAYER INGESTION")
            logger.info("=" * 80)
            
            ingest_year_data(
                spark=self.spark,
                year=self.year,
                catalog=self.catalog,
                schema=self.schema
            )
            
            logger.info("✓ Bronze layer ingestion completed successfully")
            return True
            
        except Exception as e:
            logger.error(f"✗ Bronze layer ingestion failed: {str(e)}", exc_info=True)
            return False
    
    def run_silver_layer(self) -> bool:
        """Execute Silver layer transformation"""
        try:
            logger.info("=" * 80)
            logger.info("STARTING SILVER LAYER TRANSFORMATION")
            logger.info("=" * 80)
            
            # Get configuration or use defaults
            key_columns = self.config.get("key_columns", ["trip_id"])
            date_columns = self.config.get("date_columns", ["pickup_datetime", "dropoff_datetime"])
            partition_cols = date_columns[:1] if date_columns else ["processing_date"]
            
            transform_bronze_to_silver(
                spark=self.spark,
                bronze_table=f"gm_data_{self.year}",
                silver_table=f"gm_data_{self.year}",
                key_columns=key_columns,
                string_columns=None,  # Update with actual string columns
                null_rules=None,  # Update with actual null handling rules
                partition_cols=partition_cols
            )
            
            logger.info("✓ Silver layer transformation completed successfully")
            return True
            
        except Exception as e:
            logger.error(f"✗ Silver layer transformation failed: {str(e)}", exc_info=True)
            return False
    
    def run_gold_layer(self) -> bool:
        """Execute Gold layer aggregation"""
        try:
            logger.info("=" * 80)
            logger.info("STARTING GOLD LAYER AGGREGATION")
            logger.info("=" * 80)
            
            # Get configuration or use defaults
            date_columns = self.config.get("date_columns", ["pickup_datetime"])
            date_column = date_columns[0] if date_columns else "pickup_datetime"
            metric_columns = self.config.get("metric_columns", ["fare_amount", "tip_amount", "total_amount"])
            dimension_columns = self.config.get("dimension_columns", ["passenger_count", "payment_type"])
            
            create_gold_aggregations(
                spark=self.spark,
                silver_table=f"gm_data_{self.year}",
                date_column=date_column,
                metric_columns=metric_columns,
                dimension_columns=dimension_columns
            )
            
            logger.info("✓ Gold layer aggregation completed successfully")
            return True
            
        except Exception as e:
            logger.error(f"✗ Gold layer aggregation failed: {str(e)}", exc_info=True)
            return False
    
    def run_validation(self) -> bool:
        """Execute data quality validation"""
        try:
            logger.info("=" * 80)
            logger.info("STARTING DATA QUALITY VALIDATION")
            logger.info("=" * 80)
            
            run_full_validation_suite(
                spark=self.spark,
                year=self.year
            )
            
            logger.info("✓ Data quality validation completed successfully")
            return True
            
        except Exception as e:
            logger.error(f"✗ Data quality validation failed: {str(e)}", exc_info=True)
            return False
    
    def run_optimization(self, vacuum: bool = False) -> bool:
        """Execute table optimization"""
        try:
            logger.info("=" * 80)
            logger.info("STARTING TABLE OPTIMIZATION")
            logger.info("=" * 80)
            
            run_maintenance_job(
                spark=self.spark,
                year=self.year,
                vacuum=vacuum,
                retention_hours=168  # 7 days
            )
            
            logger.info("✓ Table optimization completed successfully")
            return True
            
        except Exception as e:
            logger.error(f"✗ Table optimization failed: {str(e)}", exc_info=True)
            return False
    
    def run_analytics(self) -> bool:
        """Generate analytics and visualizations"""
        try:
            logger.info("=" * 80)
            logger.info("STARTING ANALYTICS GENERATION")
            logger.info("=" * 80)
            
            create_visualizations(
                spark=self.spark,
                year=self.year
            )
            
            logger.info("✓ Analytics generation completed successfully")
            return True
            
        except Exception as e:
            logger.error(f"✗ Analytics generation failed: {str(e)}", exc_info=True)
            return False
    
    def run_full_pipeline(
        self,
        skip_bronze: bool = False,
        skip_validation: bool = False,
        vacuum: bool = False
    ) -> dict:
        """
        Execute the complete pipeline
        
        Args:
            skip_bronze: Skip bronze layer ingestion
            skip_validation: Skip data quality validation
            vacuum: Run VACUUM during optimization
        
        Returns:
            Dictionary with execution results
        """
        start_time = datetime.now()
        results = {
            "start_time": start_time,
            "year": self.year,
            "steps": {}
        }
        
        logger.info("=" * 80)
        logger.info(f"STARTING COMPLETE MEDALLION PIPELINE - {self.year}")
        logger.info("=" * 80)
        
        # Execute pipeline steps
        if not skip_bronze:
            results["steps"]["bronze"] = self.run_bronze_layer()
            if not results["steps"]["bronze"]:
                logger.error("Pipeline halted due to Bronze layer failure")
                return self._finalize_results(results, start_time)
        
        results["steps"]["silver"] = self.run_silver_layer()
        if not results["steps"]["silver"]:
            logger.error("Pipeline halted due to Silver layer failure")
            return self._finalize_results(results, start_time)
        
        if not skip_validation:
            results["steps"]["validation"] = self.run_validation()
        
        results["steps"]["gold"] = self.run_gold_layer()
        if not results["steps"]["gold"]:
            logger.error("Pipeline halted due to Gold layer failure")
            return self._finalize_results(results, start_time)
        
        results["steps"]["optimization"] = self.run_optimization(vacuum=vacuum)
        
        results["steps"]["analytics"] = self.run_analytics()
        
        return self._finalize_results(results, start_time)
    
    def _finalize_results(self, results: dict, start_time: datetime) -> dict:
        """Finalize and log pipeline results"""
        end_time = datetime.now()
        duration = end_time - start_time
        
        results["end_time"] = end_time
        results["duration_seconds"] = duration.total_seconds()
        results["duration_minutes"] = duration.total_seconds() / 60
        results["success"] = all(results.get("steps", {}).values())
        
        logger.info("=" * 80)
        logger.info("PIPELINE EXECUTION SUMMARY")
        logger.info("=" * 80)
        logger.info(f"Year:           {results['year']}")
        logger.info(f"Start Time:     {results['start_time']}")
        logger.info(f"End Time:       {results['end_time']}")
        logger.info(f"Duration:       {results['duration_minutes']:.2f} minutes")
        logger.info(f"Overall Status: {'✓ SUCCESS' if results['success'] else '✗ FAILED'}")
        logger.info("")
        logger.info("Step Results:")
        for step, success in results.get("steps", {}).items():
            status = "✓ PASSED" if success else "✗ FAILED"
            logger.info(f"  {step:15} {status}")
        logger.info("=" * 80)
        
        return results


def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(
        description="Run GM Medallion Architecture Pipeline"
    )
    parser.add_argument(
        "--year",
        type=str,
        required=True,
        help="Year to process (e.g., 2023)"
    )
    parser.add_argument(
        "--catalog",
        type=str,
        default="gm_demo",
        help="Unity Catalog name"
    )
    parser.add_argument(
        "--schema",
        type=str,
        default="gm_test_schema",
        help="Schema name"
    )
    parser.add_argument(
        "--skip-bronze",
        action="store_true",
        help="Skip bronze layer ingestion"
    )
    parser.add_argument(
        "--skip-validation",
        action="store_true",
        help="Skip data quality validation"
    )
    parser.add_argument(
        "--vacuum",
        action="store_true",
        help="Run VACUUM during optimization"
    )
    parser.add_argument(
        "--step",
        type=str,
        choices=["bronze", "silver", "gold", "validation", "optimization", "analytics"],
        help="Run only a specific step"
    )
    parser.add_argument(
        "--config",
        type=str,
        default="config/data_discovery_sample.json",
        help="Path to data discovery configuration JSON file"
    )
    
    args = parser.parse_args()
    
    # Load configuration
    config = {}
    if os.path.exists(args.config):
        try:
            with open(args.config, 'r') as f:
                config = json.load(f)
            logger.info(f"Configuration loaded from {args.config}")
        except Exception as e:
            logger.warning(f"Failed to load config from {args.config}: {e}")
    else:
        logger.warning(f"Config file not found: {args.config}, using defaults")
    
    # Create pipeline instance
    pipeline = MedallionPipeline(
        year=args.year,
        catalog=args.catalog,
        schema=args.schema,
        config=config
    )
    
    # Execute pipeline
    if args.step:
        # Run single step
        step_methods = {
            "bronze": pipeline.run_bronze_layer,
            "silver": pipeline.run_silver_layer,
            "gold": pipeline.run_gold_layer,
            "validation": pipeline.run_validation,
            "optimization": lambda: pipeline.run_optimization(vacuum=args.vacuum),
            "analytics": pipeline.run_analytics
        }
        
        success = step_methods[args.step]()
        exit(0 if success else 1)
    else:
        # Run full pipeline
        results = pipeline.run_full_pipeline(
            skip_bronze=args.skip_bronze,
            skip_validation=args.skip_validation,
            vacuum=args.vacuum
        )
        
        exit(0 if results["success"] else 1)


if __name__ == "__main__":
    main()
