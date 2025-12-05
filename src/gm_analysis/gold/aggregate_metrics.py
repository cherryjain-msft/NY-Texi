"""
Gold Layer Aggregation Module

Purpose: Create business-ready aggregated datasets
- Build time-series aggregations
- Calculate business metrics and KPIs
- Create dimensional models
- Optimize for query performance
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, sum as spark_sum, count, avg, min as spark_min, max as spark_max,
    date_trunc, year, month, dayofmonth, hour,
    current_timestamp, lit, round as spark_round,
    countDistinct, first, last, collect_list, collect_set,
    when, coalesce, expr
)
from pyspark.sql.types import *
from delta.tables import DeltaTable
from typing import List, Optional, Dict
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class GoldAggregation:
    """Handle business-level aggregations for Gold layer"""
    
    def __init__(
        self,
        spark: SparkSession,
        catalog: str = "gm_demo",
        schema: str = "gm_test_schema"
    ):
        self.spark = spark
        self.catalog = catalog
        self.schema = schema
    
    def create_daily_aggregation(
        self,
        df: DataFrame,
        date_column: str,
        metric_columns: List[str],
        dimension_columns: Optional[List[str]] = None
    ) -> DataFrame:
        """
        Create daily aggregated metrics
        
        Args:
            df: Input DataFrame
            date_column: Column containing date/timestamp
            metric_columns: Numeric columns to aggregate
            dimension_columns: Optional categorical columns for grouping
        
        Returns:
            Daily aggregated DataFrame
        """
        logger.info("Creating daily aggregations")
        
        # Ensure date column is date type
        df = df.withColumn("agg_date", col(date_column).cast(DateType()))
        
        # Build grouping columns
        group_cols = ["agg_date"]
        if dimension_columns:
            group_cols.extend(dimension_columns)
        
        # Build aggregations
        agg_exprs = []
        for metric in metric_columns:
            agg_exprs.extend([
                spark_sum(col(metric)).alias(f"{metric}_sum"),
                avg(col(metric)).alias(f"{metric}_avg"),
                spark_min(col(metric)).alias(f"{metric}_min"),
                spark_max(col(metric)).alias(f"{metric}_max"),
                count(col(metric)).alias(f"{metric}_count")
            ])
        
        # Add record count
        agg_exprs.append(count("*").alias("record_count"))
        
        # Perform aggregation
        df_daily = df.groupBy(*group_cols).agg(*agg_exprs)
        
        # Add metadata
        df_daily = df_daily.withColumn("_gold_processed_timestamp", current_timestamp())
        df_daily = df_daily.withColumn("aggregation_level", lit("daily"))
        
        return df_daily
    
    def create_monthly_aggregation(
        self,
        df: DataFrame,
        date_column: str,
        metric_columns: List[str],
        dimension_columns: Optional[List[str]] = None
    ) -> DataFrame:
        """
        Create monthly aggregated metrics
        
        Args:
            df: Input DataFrame
            date_column: Column containing date/timestamp
            metric_columns: Numeric columns to aggregate
            dimension_columns: Optional categorical columns for grouping
        
        Returns:
            Monthly aggregated DataFrame
        """
        logger.info("Creating monthly aggregations")
        
        # Extract year and month
        df = df.withColumn("agg_year", year(col(date_column)))
        df = df.withColumn("agg_month", month(col(date_column)))
        
        # Build grouping columns
        group_cols = ["agg_year", "agg_month"]
        if dimension_columns:
            group_cols.extend(dimension_columns)
        
        # Build aggregations
        agg_exprs = []
        for metric in metric_columns:
            agg_exprs.extend([
                spark_sum(col(metric)).alias(f"{metric}_sum"),
                avg(col(metric)).alias(f"{metric}_avg"),
                spark_min(col(metric)).alias(f"{metric}_min"),
                spark_max(col(metric)).alias(f"{metric}_max"),
                count(col(metric)).alias(f"{metric}_count")
            ])
        
        agg_exprs.append(count("*").alias("record_count"))
        
        # Perform aggregation
        df_monthly = df.groupBy(*group_cols).agg(*agg_exprs)
        
        # Add metadata
        df_monthly = df_monthly.withColumn("_gold_processed_timestamp", current_timestamp())
        df_monthly = df_monthly.withColumn("aggregation_level", lit("monthly"))
        
        return df_monthly
    
    def create_kpi_table(
        self,
        df: DataFrame,
        kpi_definitions: Dict[str, str]
    ) -> DataFrame:
        """
        Create KPI calculations
        
        Args:
            df: Input DataFrame
            kpi_definitions: Dict mapping KPI names to SQL expressions
        
        Returns:
            DataFrame with calculated KPIs
        """
        logger.info(f"Calculating {len(kpi_definitions)} KPIs")
        
        for kpi_name, kpi_expr in kpi_definitions.items():
            df = df.withColumn(kpi_name, expr(kpi_expr))
        
        return df
    
    def create_dimension_table(
        self,
        df: DataFrame,
        dimension_columns: List[str],
        include_counts: bool = True
    ) -> DataFrame:
        """
        Create a dimension table with unique combinations
        
        Args:
            df: Input DataFrame
            dimension_columns: Columns to include in dimension
            include_counts: Whether to include record counts
        
        Returns:
            Dimension DataFrame
        """
        logger.info(f"Creating dimension table with {len(dimension_columns)} columns")
        
        if include_counts:
            dim_df = df.groupBy(*dimension_columns) \
                .agg(count("*").alias("record_count")) \
                .orderBy(*dimension_columns)
        else:
            dim_df = df.select(*dimension_columns).distinct().orderBy(*dimension_columns)
        
        # Add surrogate key
        from pyspark.sql.window import Window
        from pyspark.sql.functions import row_number
        
        window = Window.orderBy(*dimension_columns)
        dim_df = dim_df.withColumn("dimension_key", row_number().over(window))
        
        return dim_df
    
    def write_gold_table(
        self,
        df: DataFrame,
        table_name: str,
        partition_cols: Optional[List[str]] = None,
        z_order_cols: Optional[List[str]] = None,
        mode: str = "overwrite"
    ) -> None:
        """
        Write data to Gold Delta table with optimizations
        
        Args:
            df: DataFrame to write
            table_name: Name of gold table (will be prefixed with gold_)
            partition_cols: Optional partition columns
            z_order_cols: Optional columns for Z-ordering
            mode: Write mode (overwrite, append, merge)
        """
        gold_table = f"{self.catalog}.{self.schema}.gold_{table_name}"
        
        logger.info(f"Writing to {gold_table}")
        
        try:
            # Write table
            writer = df.write \
                .format("delta") \
                .mode(mode)
            
            if partition_cols:
                writer = writer.partitionBy(*partition_cols)
            
            writer \
                .option("delta.autoOptimize.optimizeWrite", "true") \
                .option("delta.autoOptimize.autoCompact", "true") \
                .saveAsTable(gold_table)
            
            logger.info(f"Successfully wrote {df.count():,} rows to {gold_table}")
            
            # Add table comment
            self.spark.sql(f"""
                COMMENT ON TABLE {gold_table} IS 
                'Gold layer - Business-ready aggregated data optimized for analytics and reporting.'
            """)
            
            # Optimize and Z-order
            if z_order_cols:
                logger.info(f"Z-ordering by: {z_order_cols}")
                z_order_str = ", ".join(z_order_cols)
                self.spark.sql(f"OPTIMIZE {gold_table} ZORDER BY ({z_order_str})")
            else:
                logger.info("Optimizing table")
                self.spark.sql(f"OPTIMIZE {gold_table}")
            
            # Analyze table for statistics
            logger.info("Analyzing table statistics")
            self.spark.sql(f"ANALYZE TABLE {gold_table} COMPUTE STATISTICS FOR ALL COLUMNS")
            
        except Exception as e:
            logger.error(f"Error writing to {gold_table}: {str(e)}")
            raise


def create_gold_aggregations(
    spark: SparkSession,
    silver_table: str,
    date_column: str,
    metric_columns: List[str],
    dimension_columns: Optional[List[str]] = None
) -> None:
    """
    Create complete set of gold aggregations
    
    Args:
        spark: SparkSession
        silver_table: Name of silver table (without schema prefix)
        date_column: Date/timestamp column for aggregation
        metric_columns: Numeric columns to aggregate
        dimension_columns: Optional categorical columns
    """
    logger.info(f"Creating gold aggregations from silver_{silver_table}")
    
    gold = GoldAggregation(spark)
    
    # Read silver table
    silver_full_name = f"{gold.catalog}.{gold.schema}.silver_{silver_table}"
    df = spark.table(silver_full_name)
    
    logger.info(f"Read {df.count():,} rows from {silver_full_name}")
    
    # Create daily aggregation
    df_daily = gold.create_daily_aggregation(
        df=df,
        date_column=date_column,
        metric_columns=metric_columns,
        dimension_columns=dimension_columns
    )
    
    gold.write_gold_table(
        df=df_daily,
        table_name=f"{silver_table}_daily",
        partition_cols=["agg_date"],
        z_order_cols=dimension_columns if dimension_columns else None
    )
    
    # Create monthly aggregation
    df_monthly = gold.create_monthly_aggregation(
        df=df,
        date_column=date_column,
        metric_columns=metric_columns,
        dimension_columns=dimension_columns
    )
    
    gold.write_gold_table(
        df=df_monthly,
        table_name=f"{silver_table}_monthly",
        partition_cols=["agg_year", "agg_month"],
        z_order_cols=dimension_columns if dimension_columns else None
    )
    
    # Create dimension table if dimensions provided
    if dimension_columns:
        df_dim = gold.create_dimension_table(
            df=df,
            dimension_columns=dimension_columns,
            include_counts=True
        )
        
        gold.write_gold_table(
            df=df_dim,
            table_name=f"{silver_table}_dimensions",
            mode="overwrite"
        )
    
    logger.info("Completed gold layer aggregations")


if __name__ == "__main__":
    # Example usage
    spark = SparkSession.builder.getOrCreate()
    
    # Create gold aggregations for 2023 data
    create_gold_aggregations(
        spark=spark,
        silver_table="gm_data_2023",
        date_column="transaction_date",  # Update with actual column
        metric_columns=["amount", "quantity"],  # Update with actual columns
        dimension_columns=["category", "region"]  # Update with actual columns
    )
