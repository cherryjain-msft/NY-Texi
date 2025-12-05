"""
Bronze Layer Ingestion Module

Purpose: Ingest raw data from volumes into Bronze Delta tables
- Preserve original data format exactly as-is
- Add metadata columns for audit and lineage
- Enable incremental ingestion
- ACID guarantees with Delta Lake
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    current_timestamp, input_file_name, col, lit, 
    to_timestamp, date_format, year, month, dayofmonth
)
from pyspark.sql.types import StructType
from delta.tables import DeltaTable
from typing import Optional, List
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class BronzeIngestion:
    """Handle raw data ingestion to Bronze layer"""
    
    def __init__(
        self,
        spark: SparkSession,
        catalog: str = "gm_demo",
        schema: str = "gm_test_schema",
        source_path: str = "/Volumes/gm_demo/gm_test_schema/gm_volume"
    ):
        self.spark = spark
        self.catalog = catalog
        self.schema = schema
        self.source_path = source_path
        
    def add_metadata_columns(self, df: DataFrame) -> DataFrame:
        """Add standard metadata columns to ingested data"""
        return df \
            .withColumn("_ingestion_timestamp", current_timestamp()) \
            .withColumn("_source_file", input_file_name()) \
            .withColumn("_source_modified_time", current_timestamp())
    
    def ingest_to_bronze(
        self,
        source_file_path: str,
        table_name: str,
        file_format: str = "parquet",
        schema: Optional[StructType] = None,
        partition_cols: Optional[List[str]] = None,
        options: Optional[dict] = None
    ) -> None:
        """
        Ingest data from source to Bronze Delta table
        
        Args:
            source_file_path: Path to source data files
            table_name: Name of the Bronze table (will be prefixed with bronze_)
            file_format: Format of source files (parquet, csv, json)
            schema: Optional schema for reading data
            partition_cols: Optional list of columns to partition by
            options: Optional dict of reader options
        """
        bronze_table = f"{self.catalog}.{self.schema}.bronze_{table_name}"
        
        logger.info(f"Starting ingestion to {bronze_table}")
        logger.info(f"Source: {source_file_path}")
        
        try:
            # Read source data
            reader = self.spark.read.format(file_format)
            
            if options:
                for key, value in options.items():
                    reader = reader.option(key, value)
            
            if schema:
                reader = reader.schema(schema)
            
            df = reader.load(source_file_path)
            
            # Add metadata columns
            df_with_metadata = self.add_metadata_columns(df)
            
            # Check if table exists
            if self.spark.catalog.tableExists(bronze_table):
                logger.info(f"Appending to existing table: {bronze_table}")
                
                # Append to existing table
                writer = df_with_metadata.write \
                    .format("delta") \
                    .mode("append")
                
                if partition_cols:
                    writer = writer.partitionBy(*partition_cols)
                
                writer.saveAsTable(bronze_table)
                
            else:
                logger.info(f"Creating new table: {bronze_table}")
                
                # Create new table
                writer = df_with_metadata.write \
                    .format("delta") \
                    .mode("overwrite")
                
                if partition_cols:
                    writer = writer.partitionBy(*partition_cols)
                
                # Set table properties
                writer = writer \
                    .option("delta.enableChangeDataFeed", "true") \
                    .option("delta.autoOptimize.optimizeWrite", "true") \
                    .option("delta.autoOptimize.autoCompact", "true")
                
                writer.saveAsTable(bronze_table)
            
            # Get row count
            row_count = df_with_metadata.count()
            logger.info(f"Successfully ingested {row_count:,} rows to {bronze_table}")
            
            # Add table comments
            self.spark.sql(f"""
                COMMENT ON TABLE {bronze_table} IS 
                'Bronze layer - Raw data ingested from {source_file_path}. 
                Append-only table with audit metadata.'
            """)
            
        except Exception as e:
            logger.error(f"Error ingesting to {bronze_table}: {str(e)}")
            raise
    
    def ingest_with_copy_into(
        self,
        source_file_path: str,
        table_name: str,
        file_format: str = "parquet",
        format_options: Optional[dict] = None,
        copy_options: Optional[dict] = None
    ) -> None:
        """
        Use COPY INTO for scalable, idempotent ingestion
        
        Args:
            source_file_path: Path to source data files
            table_name: Name of the Bronze table (will be prefixed with bronze_)
            file_format: Format of source files
            format_options: Options for file format
            copy_options: Options for COPY INTO operation
        """
        bronze_table = f"{self.catalog}.{self.schema}.bronze_{table_name}"
        
        logger.info(f"Using COPY INTO for {bronze_table}")
        
        try:
            # Build format options string
            format_opts = ""
            if format_options:
                opts = [f"{k} = '{v}'" for k, v in format_options.items()]
                format_opts = f"FORMAT_OPTIONS ({', '.join(opts)})"
            
            # Build copy options string
            copy_opts = ""
            if copy_options:
                opts = [f"{k} = '{v}'" for k, v in copy_options.items()]
                copy_opts = f"COPY_OPTIONS ({', '.join(opts)})"
            
            # Execute COPY INTO
            sql = f"""
                COPY INTO {bronze_table}
                FROM '{source_file_path}'
                FILEFORMAT = {file_format}
                {format_opts}
                {copy_opts}
            """
            
            logger.info(f"Executing: {sql}")
            result = self.spark.sql(sql)
            result.show()
            
            logger.info(f"Successfully completed COPY INTO for {bronze_table}")
            
        except Exception as e:
            logger.error(f"Error in COPY INTO for {bronze_table}: {str(e)}")
            raise
    
    def create_bronze_table_from_schema(
        self,
        table_name: str,
        schema: StructType,
        partition_cols: Optional[List[str]] = None
    ) -> None:
        """
        Create an empty Bronze table with specified schema
        
        Args:
            table_name: Name of the Bronze table (will be prefixed with bronze_)
            schema: Schema for the table
            partition_cols: Optional list of columns to partition by
        """
        bronze_table = f"{self.catalog}.{self.schema}.bronze_{table_name}"
        
        logger.info(f"Creating empty Bronze table: {bronze_table}")
        
        try:
            # Create empty DataFrame with schema
            df = self.spark.createDataFrame([], schema)
            df_with_metadata = self.add_metadata_columns(df)
            
            # Create table
            writer = df_with_metadata.write \
                .format("delta") \
                .mode("overwrite")
            
            if partition_cols:
                writer = writer.partitionBy(*partition_cols)
            
            writer \
                .option("delta.enableChangeDataFeed", "true") \
                .option("delta.autoOptimize.optimizeWrite", "true") \
                .option("delta.autoOptimize.autoCompact", "true") \
                .saveAsTable(bronze_table)
            
            logger.info(f"Successfully created {bronze_table}")
            
        except Exception as e:
            logger.error(f"Error creating {bronze_table}: {str(e)}")
            raise


def ingest_year_data(
    spark: SparkSession,
    year: str,
    catalog: str = "gm_demo",
    schema: str = "gm_test_schema"
) -> None:
    """
    Ingest all data for a specific year
    
    Args:
        spark: SparkSession
        year: Year to ingest (e.g., "2023")
        catalog: Unity Catalog name
        schema: Schema name
    """
    logger.info(f"Starting ingestion for year {year}")
    
    ingestion = BronzeIngestion(
        spark=spark,
        catalog=catalog,
        schema=schema
    )
    
    base_path = f"/Volumes/{catalog}/{schema}/gm_volume/{year}"
    
    # List files/directories in year folder
    try:
        files = spark.sparkContext._jvm.org.apache.hadoop.fs.FileSystem.get(
            spark.sparkContext._jsc.hadoopConfiguration()
        ).listStatus(
            spark.sparkContext._jvm.org.apache.hadoop.fs.Path(base_path)
        )
        
        for file_status in files:
            file_path = str(file_status.getPath())
            logger.info(f"Processing: {file_path}")
            
            # Determine file format and ingest
            if file_path.endswith('.parquet') or '/parquet/' in file_path:
                table_name = f"gm_data_{year}"
                ingestion.ingest_to_bronze(
                    source_file_path=file_path,
                    table_name=table_name,
                    file_format="parquet",
                    partition_cols=["_ingestion_date"]
                )
            elif file_path.endswith('.csv') or '/csv/' in file_path:
                table_name = f"gm_data_{year}_csv"
                ingestion.ingest_to_bronze(
                    source_file_path=file_path,
                    table_name=table_name,
                    file_format="csv",
                    options={"header": "true", "inferSchema": "true"},
                    partition_cols=["_ingestion_date"]
                )
    
    except Exception as e:
        logger.error(f"Error processing year {year}: {str(e)}")
        raise
    
    logger.info(f"Completed ingestion for year {year}")


if __name__ == "__main__":
    # Example usage
    spark = SparkSession.builder.getOrCreate()
    
    # Ingest 2023 data
    ingest_year_data(spark, "2023")
