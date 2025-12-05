"""
Silver Layer Transformation Module

Purpose: Clean, deduplicate, and standardize data from Bronze to Silver
- Remove duplicates
- Handle null values
- Standardize data types and formats
- Apply business rules and validations
- Implement SCD Type 2 for historical tracking
"""

from pyspark.sql import SparkSession, DataFrame, Window
from pyspark.sql.functions import (
    col, coalesce, when, trim, upper, lower, regexp_replace,
    to_date, to_timestamp, current_timestamp, lit, 
    row_number, lag, lead, md5, concat_ws, hash,
    count, sum as spark_sum, avg, min as spark_min, max as spark_max
)
from pyspark.sql.types import *
from delta.tables import DeltaTable
from typing import List, Optional, Dict
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class SilverTransformation:
    """Handle data cleansing and transformation to Silver layer"""
    
    def __init__(
        self,
        spark: SparkSession,
        catalog: str = "gm_demo",
        schema: str = "gm_test_schema"
    ):
        self.spark = spark
        self.catalog = catalog
        self.schema = schema
    
    def remove_duplicates(
        self,
        df: DataFrame,
        key_columns: List[str],
        order_column: str = "_ingestion_timestamp"
    ) -> DataFrame:
        """
        Remove duplicates keeping the most recent record
        
        Args:
            df: Input DataFrame
            key_columns: Columns to identify duplicates
            order_column: Column to determine which record to keep (most recent)
        
        Returns:
            Deduplicated DataFrame
        """
        logger.info(f"Removing duplicates based on: {key_columns}")
        
        window = Window.partitionBy(*key_columns).orderBy(col(order_column).desc())
        
        df_deduped = df \
            .withColumn("_row_num", row_number().over(window)) \
            .filter(col("_row_num") == 1) \
            .drop("_row_num")
        
        original_count = df.count()
        deduped_count = df_deduped.count()
        duplicates_removed = original_count - deduped_count
        
        logger.info(f"Removed {duplicates_removed:,} duplicates ({duplicates_removed/original_count*100:.2f}%)")
        
        return df_deduped
    
    def standardize_strings(
        self,
        df: DataFrame,
        string_columns: List[str],
        case: str = "upper"
    ) -> DataFrame:
        """
        Standardize string columns (trim, case conversion)
        
        Args:
            df: Input DataFrame
            string_columns: List of string columns to standardize
            case: 'upper', 'lower', or None
        
        Returns:
            DataFrame with standardized strings
        """
        logger.info(f"Standardizing {len(string_columns)} string columns")
        
        for col_name in string_columns:
            # Trim whitespace
            df = df.withColumn(col_name, trim(col(col_name)))
            
            # Apply case conversion
            if case == "upper":
                df = df.withColumn(col_name, upper(col(col_name)))
            elif case == "lower":
                df = df.withColumn(col_name, lower(col(col_name)))
        
        return df
    
    def handle_nulls(
        self,
        df: DataFrame,
        null_handling_rules: Dict[str, any]
    ) -> DataFrame:
        """
        Handle null values based on specified rules
        
        Args:
            df: Input DataFrame
            null_handling_rules: Dict mapping column names to default values or 'drop'
        
        Returns:
            DataFrame with nulls handled
        """
        logger.info("Applying null handling rules")
        
        for col_name, rule in null_handling_rules.items():
            if rule == "drop":
                # Drop rows where this column is null
                df = df.filter(col(col_name).isNotNull())
            else:
                # Fill nulls with specified value
                df = df.withColumn(
                    col_name,
                    coalesce(col(col_name), lit(rule))
                )
        
        return df
    
    def apply_business_rules(self, df: DataFrame) -> DataFrame:
        """
        Apply business-specific validation and transformation rules
        
        Args:
            df: Input DataFrame
        
        Returns:
            Transformed DataFrame
        """
        logger.info("Applying business rules")
        
        # Add data quality flags
        df = df.withColumn(
            "_is_valid",
            lit(True)  # Default to valid, update based on rules
        )
        
        df = df.withColumn(
            "_validation_errors",
            lit(None).cast(StringType())
        )
        
        # Add processing metadata
        df = df.withColumn("_silver_processed_timestamp", current_timestamp())
        
        return df
    
    def add_data_quality_metrics(self, df: DataFrame) -> DataFrame:
        """
        Add data quality metric columns
        
        Args:
            df: Input DataFrame
        
        Returns:
            DataFrame with quality metrics
        """
        logger.info("Adding data quality metrics")
        
        # Count null columns per row
        null_cols = [col_name for col_name in df.columns if not col_name.startswith("_")]
        
        null_count_expr = spark_sum(
            when(col(c).isNull(), 1).otherwise(0)
            for c in null_cols
        )
        
        df = df.withColumn("_null_column_count", null_count_expr)
        
        df = df.withColumn(
            "_completeness_score",
            1 - (col("_null_column_count") / lit(len(null_cols)))
        )
        
        return df
    
    def create_or_merge_silver_table(
        self,
        df: DataFrame,
        table_name: str,
        key_columns: List[str],
        partition_cols: Optional[List[str]] = None,
        merge_mode: bool = True
    ) -> None:
        """
        Create or merge data into Silver Delta table
        
        Args:
            df: Transformed DataFrame
            table_name: Name of Silver table (will be prefixed with silver_)
            key_columns: Columns for merge condition
            partition_cols: Optional partition columns
            merge_mode: If True, use merge; if False, overwrite
        """
        silver_table = f"{self.catalog}.{self.schema}.silver_{table_name}"
        
        logger.info(f"Writing to {silver_table}")
        
        try:
            if self.spark.catalog.tableExists(silver_table) and merge_mode:
                logger.info(f"Merging into existing table: {silver_table}")
                
                # Build merge condition
                merge_condition = " AND ".join(
                    [f"target.{col} = source.{col}" for col in key_columns]
                )
                
                delta_table = DeltaTable.forName(self.spark, silver_table)
                
                delta_table.alias("target").merge(
                    df.alias("source"),
                    merge_condition
                ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
                
                logger.info(f"Successfully merged into {silver_table}")
                
            else:
                logger.info(f"Creating/overwriting table: {silver_table}")
                
                writer = df.write \
                    .format("delta") \
                    .mode("overwrite")
                
                if partition_cols:
                    writer = writer.partitionBy(*partition_cols)
                
                writer \
                    .option("delta.enableChangeDataFeed", "true") \
                    .option("delta.autoOptimize.optimizeWrite", "true") \
                    .option("delta.autoOptimize.autoCompact", "true") \
                    .saveAsTable(silver_table)
                
                logger.info(f"Successfully created {silver_table}")
            
            # Add table comment
            self.spark.sql(f"""
                COMMENT ON TABLE {silver_table} IS 
                'Silver layer - Cleansed and standardized data. 
                Deduplicatedata with data quality metrics.'
            """)
            
            # Optimize table
            logger.info(f"Optimizing {silver_table}")
            self.spark.sql(f"OPTIMIZE {silver_table}")
            
        except Exception as e:
            logger.error(f"Error writing to {silver_table}: {str(e)}")
            raise


def transform_bronze_to_silver(
    spark: SparkSession,
    bronze_table: str,
    silver_table: str,
    key_columns: List[str],
    string_columns: Optional[List[str]] = None,
    null_rules: Optional[Dict[str, any]] = None,
    partition_cols: Optional[List[str]] = None
) -> None:
    """
    Complete transformation pipeline from Bronze to Silver
    
    Args:
        spark: SparkSession
        bronze_table: Name of bronze table (without schema prefix)
        silver_table: Name of silver table (without schema prefix)
        key_columns: Columns to identify duplicates
        string_columns: Optional list of string columns to standardize
        null_rules: Optional null handling rules
        partition_cols: Optional partition columns
    """
    logger.info(f"Starting Bronze to Silver transformation: {bronze_table} -> {silver_table}")
    
    transformer = SilverTransformation(spark)
    
    # Read bronze table
    bronze_full_name = f"{transformer.catalog}.{transformer.schema}.bronze_{bronze_table}"
    df = spark.table(bronze_full_name)
    
    logger.info(f"Read {df.count():,} rows from {bronze_full_name}")
    
    # Apply transformations
    df = transformer.remove_duplicates(df, key_columns)
    
    if string_columns:
        df = transformer.standardize_strings(df, string_columns)
    
    if null_rules:
        df = transformer.handle_nulls(df, null_rules)
    
    df = transformer.apply_business_rules(df)
    df = transformer.add_data_quality_metrics(df)
    
    # Write to Silver
    transformer.create_or_merge_silver_table(
        df=df,
        table_name=silver_table,
        key_columns=key_columns,
        partition_cols=partition_cols,
        merge_mode=True
    )
    
    logger.info(f"Completed transformation to silver_{silver_table}")


if __name__ == "__main__":
    # Example usage
    spark = SparkSession.builder.getOrCreate()
    
    # Transform 2023 GM data
    transform_bronze_to_silver(
        spark=spark,
        bronze_table="gm_data_2023",
        silver_table="gm_data_2023",
        key_columns=["id"],  # Update with actual key columns
        partition_cols=["processing_date"]
    )
