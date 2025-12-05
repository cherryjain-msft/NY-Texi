"""
Data Quality Validation Module

Purpose: Validate data quality across medallion architecture
- Row count validation
- Schema evolution handling
- Data quality metrics
- Anomaly detection
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, count, sum as spark_sum, avg, min as spark_min, max as spark_max,
    countDistinct, current_timestamp, lit, when, expr
)
from typing import Dict, List, Optional, Tuple
import logging
from datetime import datetime

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class DataQualityValidator:
    """Comprehensive data quality validation framework"""
    
    def __init__(
        self,
        spark: SparkSession,
        catalog: str = "gm_demo",
        schema: str = "gm_test_schema"
    ):
        self.spark = spark
        self.catalog = catalog
        self.schema = schema
        self.validation_results = []
    
    def validate_row_counts(
        self,
        source_table: str,
        target_table: str,
        tolerance_pct: float = 0.0
    ) -> Tuple[bool, Dict]:
        """
        Validate row counts between source and target tables
        
        Args:
            source_table: Source table name
            target_table: Target table name
            tolerance_pct: Acceptable variance percentage
        
        Returns:
            Tuple of (validation_passed, results_dict)
        """
        logger.info(f"Validating row counts: {source_table} -> {target_table}")
        
        source_count = self.spark.table(f"{self.catalog}.{self.schema}.{source_table}").count()
        target_count = self.spark.table(f"{self.catalog}.{self.schema}.{target_table}").count()
        
        variance = abs(source_count - target_count)
        variance_pct = (variance / source_count * 100) if source_count > 0 else 0
        
        passed = variance_pct <= tolerance_pct
        
        result = {
            "validation_type": "row_count",
            "source_table": source_table,
            "target_table": target_table,
            "source_count": source_count,
            "target_count": target_count,
            "variance": variance,
            "variance_pct": variance_pct,
            "tolerance_pct": tolerance_pct,
            "passed": passed,
            "timestamp": datetime.now()
        }
        
        self.validation_results.append(result)
        
        status = "✓ PASSED" if passed else "✗ FAILED"
        logger.info(f"{status} - Source: {source_count:,}, Target: {target_count:,}, Variance: {variance_pct:.2f}%")
        
        return passed, result
    
    def validate_schema_consistency(
        self,
        table1: str,
        table2: str,
        excluded_columns: Optional[List[str]] = None
    ) -> Tuple[bool, Dict]:
        """
        Validate schema consistency between two tables
        
        Args:
            table1: First table name
            table2: Second table name
            excluded_columns: Columns to exclude from comparison (e.g., metadata columns)
        
        Returns:
            Tuple of (validation_passed, results_dict)
        """
        logger.info(f"Validating schema consistency: {table1} vs {table2}")
        
        excluded = excluded_columns or []
        
        df1 = self.spark.table(f"{self.catalog}.{self.schema}.{table1}")
        df2 = self.spark.table(f"{self.catalog}.{self.schema}.{table2}")
        
        schema1 = {col: dtype for col, dtype in df1.dtypes if col not in excluded}
        schema2 = {col: dtype for col, dtype in df2.dtypes if col not in excluded}
        
        # Find differences
        only_in_1 = set(schema1.keys()) - set(schema2.keys())
        only_in_2 = set(schema2.keys()) - set(schema1.keys())
        
        type_mismatches = []
        for col in set(schema1.keys()) & set(schema2.keys()):
            if schema1[col] != schema2[col]:
                type_mismatches.append({
                    "column": col,
                    f"{table1}_type": schema1[col],
                    f"{table2}_type": schema2[col]
                })
        
        passed = len(only_in_1) == 0 and len(only_in_2) == 0 and len(type_mismatches) == 0
        
        result = {
            "validation_type": "schema_consistency",
            "table1": table1,
            "table2": table2,
            "only_in_table1": list(only_in_1),
            "only_in_table2": list(only_in_2),
            "type_mismatches": type_mismatches,
            "passed": passed,
            "timestamp": datetime.now()
        }
        
        self.validation_results.append(result)
        
        status = "✓ PASSED" if passed else "✗ FAILED"
        logger.info(f"{status} - Schema consistency check")
        
        if not passed:
            if only_in_1:
                logger.warning(f"Columns only in {table1}: {only_in_1}")
            if only_in_2:
                logger.warning(f"Columns only in {table2}: {only_in_2}")
            if type_mismatches:
                logger.warning(f"Type mismatches: {type_mismatches}")
        
        return passed, result
    
    def validate_null_percentages(
        self,
        table_name: str,
        column_thresholds: Dict[str, float]
    ) -> Tuple[bool, Dict]:
        """
        Validate null percentages for specified columns
        
        Args:
            table_name: Table to validate
            column_thresholds: Dict mapping column names to max acceptable null percentage
        
        Returns:
            Tuple of (validation_passed, results_dict)
        """
        logger.info(f"Validating null percentages for {table_name}")
        
        df = self.spark.table(f"{self.catalog}.{self.schema}.{table_name}")
        total_rows = df.count()
        
        column_results = []
        all_passed = True
        
        for col_name, threshold in column_thresholds.items():
            null_count = df.filter(col(col_name).isNull()).count()
            null_pct = (null_count / total_rows * 100) if total_rows > 0 else 0
            passed = null_pct <= threshold
            
            column_results.append({
                "column": col_name,
                "null_count": null_count,
                "null_pct": null_pct,
                "threshold": threshold,
                "passed": passed
            })
            
            if not passed:
                all_passed = False
                logger.warning(f"✗ {col_name}: {null_pct:.2f}% nulls (threshold: {threshold}%)")
            else:
                logger.info(f"✓ {col_name}: {null_pct:.2f}% nulls")
        
        result = {
            "validation_type": "null_percentages",
            "table_name": table_name,
            "total_rows": total_rows,
            "column_results": column_results,
            "passed": all_passed,
            "timestamp": datetime.now()
        }
        
        self.validation_results.append(result)
        
        return all_passed, result
    
    def validate_duplicates(
        self,
        table_name: str,
        key_columns: List[str],
        max_duplicate_pct: float = 0.0
    ) -> Tuple[bool, Dict]:
        """
        Validate duplicate records in table
        
        Args:
            table_name: Table to validate
            key_columns: Columns that should be unique
            max_duplicate_pct: Maximum acceptable duplicate percentage
        
        Returns:
            Tuple of (validation_passed, results_dict)
        """
        logger.info(f"Validating duplicates in {table_name} on {key_columns}")
        
        df = self.spark.table(f"{self.catalog}.{self.schema}.{table_name}")
        total_rows = df.count()
        distinct_rows = df.select(*key_columns).distinct().count()
        
        duplicate_count = total_rows - distinct_rows
        duplicate_pct = (duplicate_count / total_rows * 100) if total_rows > 0 else 0
        
        passed = duplicate_pct <= max_duplicate_pct
        
        result = {
            "validation_type": "duplicates",
            "table_name": table_name,
            "key_columns": key_columns,
            "total_rows": total_rows,
            "distinct_rows": distinct_rows,
            "duplicate_count": duplicate_count,
            "duplicate_pct": duplicate_pct,
            "max_duplicate_pct": max_duplicate_pct,
            "passed": passed,
            "timestamp": datetime.now()
        }
        
        self.validation_results.append(result)
        
        status = "✓ PASSED" if passed else "✗ FAILED"
        logger.info(f"{status} - Duplicates: {duplicate_count:,} ({duplicate_pct:.2f}%)")
        
        return passed, result
    
    def validate_data_freshness(
        self,
        table_name: str,
        timestamp_column: str,
        max_age_hours: int
    ) -> Tuple[bool, Dict]:
        """
        Validate data freshness
        
        Args:
            table_name: Table to validate
            timestamp_column: Column containing timestamp
            max_age_hours: Maximum acceptable age in hours
        
        Returns:
            Tuple of (validation_passed, results_dict)
        """
        logger.info(f"Validating data freshness for {table_name}")
        
        df = self.spark.table(f"{self.catalog}.{self.schema}.{table_name}")
        
        max_timestamp = df.agg(spark_max(col(timestamp_column))).collect()[0][0]
        
        if max_timestamp:
            from datetime import datetime, timezone
            if isinstance(max_timestamp, datetime):
                age_hours = (datetime.now(timezone.utc).replace(tzinfo=None) - max_timestamp).total_seconds() / 3600
            else:
                age_hours = -1  # Unable to calculate
            
            passed = age_hours <= max_age_hours if age_hours >= 0 else False
        else:
            age_hours = -1
            passed = False
        
        result = {
            "validation_type": "data_freshness",
            "table_name": table_name,
            "timestamp_column": timestamp_column,
            "max_timestamp": str(max_timestamp),
            "age_hours": age_hours,
            "max_age_hours": max_age_hours,
            "passed": passed,
            "timestamp": datetime.now()
        }
        
        self.validation_results.append(result)
        
        status = "✓ PASSED" if passed else "✗ FAILED"
        logger.info(f"{status} - Data age: {age_hours:.1f} hours")
        
        return passed, result
    
    def get_validation_summary(self) -> DataFrame:
        """
        Get summary of all validations as DataFrame
        
        Returns:
            DataFrame with validation results
        """
        if not self.validation_results:
            logger.warning("No validation results available")
            return None
        
        return self.spark.createDataFrame(self.validation_results)
    
    def save_validation_results(self, output_table: str) -> None:
        """
        Save validation results to a Delta table
        
        Args:
            output_table: Name of table to save results (without schema prefix)
        """
        if not self.validation_results:
            logger.warning("No validation results to save")
            return
        
        results_table = f"{self.catalog}.{self.schema}.{output_table}"
        
        logger.info(f"Saving validation results to {results_table}")
        
        df_results = self.get_validation_summary()
        
        df_results.write \
            .format("delta") \
            .mode("append") \
            .saveAsTable(results_table)
        
        logger.info(f"Saved {len(self.validation_results)} validation results")


def run_full_validation_suite(
    spark: SparkSession,
    year: str = "2023"
) -> None:
    """
    Run complete validation suite for medallion architecture
    
    Args:
        spark: SparkSession
        year: Year to validate
    """
    logger.info(f"Running full validation suite for {year}")
    
    validator = DataQualityValidator(spark)
    
    # Validate row counts across layers
    validator.validate_row_counts(
        source_table=f"bronze_gm_data_{year}",
        target_table=f"silver_gm_data_{year}",
        tolerance_pct=5.0  # Allow 5% variance for deduplication
    )
    
    # Validate schema consistency
    validator.validate_schema_consistency(
        table1=f"bronze_gm_data_{year}",
        table2=f"silver_gm_data_{year}",
        excluded_columns=["_is_valid", "_validation_errors", "_null_column_count", 
                         "_completeness_score", "_silver_processed_timestamp"]
    )
    
    # Validate null percentages in silver
    # Note: Update with actual critical columns
    validator.validate_null_percentages(
        table_name=f"silver_gm_data_{year}",
        column_thresholds={
            # "id": 0.0,  # Key columns should have 0% nulls
            # "transaction_date": 0.0,
            # "amount": 5.0  # Business columns might allow some nulls
        }
    )
    
    # Validate no duplicates in silver
    # Note: Update with actual key columns
    validator.validate_duplicates(
        table_name=f"silver_gm_data_{year}",
        key_columns=["id"],  # Update with actual key
        max_duplicate_pct=0.0
    )
    
    # Validate data freshness
    validator.validate_data_freshness(
        table_name=f"bronze_gm_data_{year}",
        timestamp_column="_ingestion_timestamp",
        max_age_hours=24
    )
    
    # Get and display summary
    summary_df = validator.get_validation_summary()
    if summary_df:
        summary_df.show(truncate=False)
        
        # Save results
        validator.save_validation_results(f"data_quality_validations")
    
    logger.info("Validation suite completed")


if __name__ == "__main__":
    spark = SparkSession.builder.getOrCreate()
    run_full_validation_suite(spark, "2023")
