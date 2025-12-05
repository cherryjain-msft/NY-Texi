"""
Delta Table Optimization Module

Purpose: Optimize Delta tables for performance
- OPTIMIZE with Z-ordering
- VACUUM old files
- ANALYZE for statistics
- Maintenance scheduling
"""

from pyspark.sql import SparkSession
from typing import List, Optional
import logging
from datetime import datetime

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class DeltaOptimizer:
    """Handle Delta table optimization operations"""
    
    def __init__(
        self,
        spark: SparkSession,
        catalog: str = "gm_demo",
        schema: str = "gm_test_schema"
    ):
        self.spark = spark
        self.catalog = catalog
        self.schema = schema
    
    def optimize_table(
        self,
        table_name: str,
        z_order_columns: Optional[List[str]] = None,
        where_clause: Optional[str] = None
    ) -> None:
        """
        Run OPTIMIZE on a Delta table
        
        Args:
            table_name: Full table name (with schema)
            z_order_columns: Optional columns for Z-ordering
            where_clause: Optional WHERE clause for selective optimization
        """
        full_table_name = f"{self.catalog}.{self.schema}.{table_name}"
        
        logger.info(f"Optimizing table: {full_table_name}")
        
        try:
            # Build OPTIMIZE command
            optimize_cmd = f"OPTIMIZE {full_table_name}"
            
            if where_clause:
                optimize_cmd += f" WHERE {where_clause}"
            
            if z_order_columns:
                z_order_str = ", ".join(z_order_columns)
                optimize_cmd += f" ZORDER BY ({z_order_str})"
                logger.info(f"Z-ordering by: {z_order_str}")
            
            logger.info(f"Executing: {optimize_cmd}")
            result = self.spark.sql(optimize_cmd)
            result.show()
            
            logger.info(f"✓ Successfully optimized {full_table_name}")
            
        except Exception as e:
            logger.error(f"✗ Error optimizing {full_table_name}: {str(e)}")
            raise
    
    def vacuum_table(
        self,
        table_name: str,
        retention_hours: int = 168,  # 7 days default
        dry_run: bool = False
    ) -> None:
        """
        Run VACUUM on a Delta table to remove old files
        
        Args:
            table_name: Full table name (with schema)
            retention_hours: Hours to retain old files (default 168 = 7 days)
            dry_run: If True, only show what would be deleted
        """
        full_table_name = f"{self.catalog}.{self.schema}.{table_name}"
        
        logger.info(f"Vacuuming table: {full_table_name} (retention: {retention_hours}h)")
        
        try:
            # Set retention check to false for custom retention periods
            if retention_hours < 168:
                logger.warning(f"Using retention period < 7 days: {retention_hours}h")
                self.spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")
            
            # Build VACUUM command
            vacuum_cmd = f"VACUUM {full_table_name}"
            
            if dry_run:
                vacuum_cmd += " DRY RUN"
                logger.info("Running in DRY RUN mode")
            
            if retention_hours != 168:
                vacuum_cmd += f" RETAIN {retention_hours} HOURS"
            
            logger.info(f"Executing: {vacuum_cmd}")
            result = self.spark.sql(vacuum_cmd)
            result.show()
            
            logger.info(f"✓ Successfully vacuumed {full_table_name}")
            
            # Reset retention check
            if retention_hours < 168:
                self.spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "true")
            
        except Exception as e:
            logger.error(f"✗ Error vacuuming {full_table_name}: {str(e)}")
            raise
    
    def analyze_table(
        self,
        table_name: str,
        compute_columns: bool = True
    ) -> None:
        """
        Run ANALYZE TABLE to compute statistics
        
        Args:
            table_name: Full table name (with schema)
            compute_columns: If True, compute column statistics
        """
        full_table_name = f"{self.catalog}.{self.schema}.{table_name}"
        
        logger.info(f"Analyzing table: {full_table_name}")
        
        try:
            if compute_columns:
                analyze_cmd = f"ANALYZE TABLE {full_table_name} COMPUTE STATISTICS FOR ALL COLUMNS"
            else:
                analyze_cmd = f"ANALYZE TABLE {full_table_name} COMPUTE STATISTICS"
            
            logger.info(f"Executing: {analyze_cmd}")
            self.spark.sql(analyze_cmd)
            
            logger.info(f"✓ Successfully analyzed {full_table_name}")
            
        except Exception as e:
            logger.error(f"✗ Error analyzing {full_table_name}: {str(e)}")
            raise
    
    def get_table_details(self, table_name: str) -> None:
        """
        Get detailed information about a Delta table
        
        Args:
            table_name: Full table name (with schema)
        """
        full_table_name = f"{self.catalog}.{self.schema}.{table_name}"
        
        logger.info(f"Getting details for: {full_table_name}")
        
        try:
            # DESCRIBE DETAIL
            logger.info("\n=== Table Details ===")
            detail = self.spark.sql(f"DESCRIBE DETAIL {full_table_name}")
            detail.show(truncate=False, vertical=True)
            
            # DESCRIBE HISTORY
            logger.info("\n=== Table History (last 10) ===")
            history = self.spark.sql(f"DESCRIBE HISTORY {full_table_name} LIMIT 10")
            history.show(truncate=False)
            
        except Exception as e:
            logger.error(f"Error getting details for {full_table_name}: {str(e)}")
            raise
    
    def optimize_all_tables(
        self,
        table_prefix: str,
        z_order_configs: Optional[dict] = None
    ) -> None:
        """
        Optimize all tables with a given prefix
        
        Args:
            table_prefix: Table name prefix (e.g., 'bronze_', 'silver_', 'gold_')
            z_order_configs: Dict mapping table names to Z-order column lists
        """
        logger.info(f"Optimizing all tables with prefix: {table_prefix}")
        
        z_order_configs = z_order_configs or {}
        
        try:
            # Get list of tables
            tables = self.spark.sql(f"""
                SHOW TABLES IN {self.catalog}.{self.schema}
            """).collect()
            
            matching_tables = [
                row.tableName for row in tables 
                if row.tableName.startswith(table_prefix)
            ]
            
            logger.info(f"Found {len(matching_tables)} tables to optimize")
            
            for table_name in matching_tables:
                z_order_cols = z_order_configs.get(table_name)
                self.optimize_table(table_name, z_order_columns=z_order_cols)
                self.analyze_table(table_name, compute_columns=True)
            
            logger.info("✓ Completed optimizing all tables")
            
        except Exception as e:
            logger.error(f"Error in batch optimization: {str(e)}")
            raise


def run_maintenance_job(
    spark: SparkSession,
    year: str = "2023",
    vacuum: bool = False,
    retention_hours: int = 168
) -> None:
    """
    Run complete maintenance job for all medallion layers
    
    Args:
        spark: SparkSession
        year: Year to process
        vacuum: Whether to run VACUUM
        retention_hours: VACUUM retention period in hours
    """
    logger.info(f"Starting maintenance job for {year}")
    logger.info(f"VACUUM enabled: {vacuum}")
    
    optimizer = DeltaOptimizer(spark)
    
    # Define Z-order configurations
    z_order_configs = {
        f"bronze_gm_data_{year}": ["_ingestion_date"],
        f"silver_gm_data_{year}": ["processing_date"],
        f"gold_gm_data_{year}_daily": ["agg_date"],
        f"gold_gm_data_{year}_monthly": ["agg_year", "agg_month"]
    }
    
    # Optimize Bronze layer
    logger.info("\n" + "="*80)
    logger.info("BRONZE LAYER MAINTENANCE")
    logger.info("="*80)
    optimizer.optimize_all_tables("bronze_", z_order_configs)
    
    # Optimize Silver layer
    logger.info("\n" + "="*80)
    logger.info("SILVER LAYER MAINTENANCE")
    logger.info("="*80)
    optimizer.optimize_all_tables("silver_", z_order_configs)
    
    # Optimize Gold layer
    logger.info("\n" + "="*80)
    logger.info("GOLD LAYER MAINTENANCE")
    logger.info("="*80)
    optimizer.optimize_all_tables("gold_", z_order_configs)
    
    # Run VACUUM if requested
    if vacuum:
        logger.info("\n" + "="*80)
        logger.info("VACUUM OPERATIONS")
        logger.info("="*80)
        
        for table_name in z_order_configs.keys():
            optimizer.vacuum_table(table_name, retention_hours=retention_hours)
    
    logger.info("\n✓ Maintenance job completed successfully")


if __name__ == "__main__":
    spark = SparkSession.builder.getOrCreate()
    
    # Run maintenance for 2023
    run_maintenance_job(
        spark=spark,
        year="2023",
        vacuum=False,  # Set to True to run VACUUM
        retention_hours=168  # 7 days
    )
