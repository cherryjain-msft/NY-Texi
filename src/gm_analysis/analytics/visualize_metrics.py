"""
Visual Analytics Module

Purpose: Create visualizations for data quality and business metrics
- Data volume trends
- Data quality metrics
- Business insights
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, count, sum as spark_sum, avg, date_format
import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns
from typing import Optional, List
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Set style for visualizations
sns.set_style("whitegrid")
plt.rcParams['figure.figsize'] = (12, 6)


class AnalyticsVisualizer:
    """Create visualizations for medallion architecture analytics"""
    
    def __init__(
        self,
        spark: SparkSession,
        catalog: str = "gm_demo",
        schema: str = "gm_test_schema"
    ):
        self.spark = spark
        self.catalog = catalog
        self.schema = schema
    
    def plot_data_volume_trends(
        self,
        table_name: str,
        date_column: str,
        title: Optional[str] = None
    ) -> None:
        """
        Plot data volume trends over time
        
        Args:
            table_name: Table to analyze
            date_column: Date column for time series
            title: Optional custom title
        """
        logger.info(f"Creating data volume trend chart for {table_name}")
        
        full_table_name = f"{self.catalog}.{self.schema}.{table_name}"
        
        # Get daily counts
        df = self.spark.sql(f"""
            SELECT 
                DATE({date_column}) as date,
                COUNT(*) as record_count
            FROM {full_table_name}
            GROUP BY DATE({date_column})
            ORDER BY date
        """)
        
        # Convert to Pandas for plotting
        pdf = df.toPandas()
        
        if pdf.empty:
            logger.warning(f"No data found in {table_name}")
            return
        
        # Create plot
        fig, ax = plt.subplots(figsize=(14, 6))
        
        ax.plot(pdf['date'], pdf['record_count'], marker='o', linewidth=2, markersize=6)
        ax.fill_between(pdf['date'], pdf['record_count'], alpha=0.3)
        
        ax.set_xlabel('Date', fontsize=12)
        ax.set_ylabel('Record Count', fontsize=12)
        ax.set_title(title or f'Data Volume Trend - {table_name}', fontsize=14, fontweight='bold')
        ax.grid(True, alpha=0.3)
        
        # Format y-axis with commas
        ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: f'{int(x):,}'))
        
        plt.xticks(rotation=45)
        plt.tight_layout()
        plt.show()
        
        logger.info("✓ Data volume trend chart created")
    
    def plot_data_quality_metrics(
        self,
        table_name: str,
        columns_to_check: Optional[List[str]] = None
    ) -> None:
        """
        Plot data quality metrics (null percentages)
        
        Args:
            table_name: Table to analyze
            columns_to_check: Optional list of columns to check
        """
        logger.info(f"Creating data quality metrics chart for {table_name}")
        
        full_table_name = f"{self.catalog}.{self.schema}.{table_name}"
        df = self.spark.table(full_table_name)
        
        total_rows = df.count()
        
        if total_rows == 0:
            logger.warning(f"No data found in {table_name}")
            return
        
        # Get columns to analyze
        if columns_to_check is None:
            columns_to_check = [c for c in df.columns if not c.startswith('_')]
        
        # Calculate null percentages
        null_stats = []
        for col_name in columns_to_check:
            null_count = df.filter(col(col_name).isNull()).count()
            null_pct = (null_count / total_rows * 100)
            null_stats.append({
                'column': col_name,
                'null_percentage': null_pct,
                'null_count': null_count
            })
        
        # Create DataFrame
        pdf = pd.DataFrame(null_stats)
        pdf = pdf.sort_values('null_percentage', ascending=True)
        
        # Create plot
        fig, ax = plt.subplots(figsize=(12, max(6, len(columns_to_check) * 0.4)))
        
        colors = ['#2ecc71' if x < 5 else '#f39c12' if x < 20 else '#e74c3c' 
                  for x in pdf['null_percentage']]
        
        ax.barh(pdf['column'], pdf['null_percentage'], color=colors, alpha=0.7)
        
        ax.set_xlabel('Null Percentage (%)', fontsize=12)
        ax.set_ylabel('Column', fontsize=12)
        ax.set_title(f'Data Quality - Null Value Analysis\n{table_name}', 
                     fontsize=14, fontweight='bold')
        ax.grid(True, axis='x', alpha=0.3)
        
        # Add value labels
        for i, (idx, row) in enumerate(pdf.iterrows()):
            ax.text(row['null_percentage'] + 0.5, i, f"{row['null_percentage']:.1f}%", 
                   va='center', fontsize=9)
        
        plt.tight_layout()
        plt.show()
        
        logger.info("✓ Data quality metrics chart created")
    
    def plot_layer_comparison(
        self,
        bronze_table: str,
        silver_table: str,
        gold_table: Optional[str] = None
    ) -> None:
        """
        Compare record counts across medallion layers
        
        Args:
            bronze_table: Bronze table name
            silver_table: Silver table name
            gold_table: Optional gold table name
        """
        logger.info("Creating medallion layer comparison chart")
        
        # Get counts
        bronze_count = self.spark.table(f"{self.catalog}.{self.schema}.{bronze_table}").count()
        silver_count = self.spark.table(f"{self.catalog}.{self.schema}.{silver_table}").count()
        
        layers = ['Bronze', 'Silver']
        counts = [bronze_count, silver_count]
        
        if gold_table:
            gold_count = self.spark.table(f"{self.catalog}.{self.schema}.{gold_table}").count()
            layers.append('Gold')
            counts.append(gold_count)
        
        # Create plot
        fig, ax = plt.subplots(figsize=(10, 6))
        
        colors = ['#95a5a6', '#3498db', '#f39c12'][:len(layers)]
        bars = ax.bar(layers, counts, color=colors, alpha=0.7, edgecolor='black', linewidth=1.5)
        
        ax.set_ylabel('Record Count', fontsize=12)
        ax.set_title('Medallion Architecture - Layer Comparison', fontsize=14, fontweight='bold')
        ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: f'{int(x):,}'))
        ax.grid(True, axis='y', alpha=0.3)
        
        # Add value labels on bars
        for bar, count in zip(bars, counts):
            height = bar.get_height()
            ax.text(bar.get_x() + bar.get_width()/2., height,
                   f'{int(count):,}',
                   ha='center', va='bottom', fontsize=11, fontweight='bold')
        
        plt.tight_layout()
        plt.show()
        
        logger.info("✓ Layer comparison chart created")
    
    def plot_aggregation_metrics(
        self,
        gold_table: str,
        date_column: str,
        metric_column: str,
        title: Optional[str] = None
    ) -> None:
        """
        Plot business metrics from gold layer
        
        Args:
            gold_table: Gold table name
            date_column: Date column for x-axis
            metric_column: Metric to plot on y-axis
            title: Optional custom title
        """
        logger.info(f"Creating business metrics chart for {gold_table}")
        
        full_table_name = f"{self.catalog}.{self.schema}.{gold_table}"
        
        df = self.spark.sql(f"""
            SELECT {date_column}, {metric_column}
            FROM {full_table_name}
            ORDER BY {date_column}
        """)
        
        pdf = df.toPandas()
        
        if pdf.empty:
            logger.warning(f"No data found in {gold_table}")
            return
        
        # Create plot
        fig, ax = plt.subplots(figsize=(14, 6))
        
        ax.plot(pdf[date_column], pdf[metric_column], 
               marker='o', linewidth=2, markersize=6, color='#3498db')
        ax.fill_between(pdf[date_column], pdf[metric_column], alpha=0.3, color='#3498db')
        
        ax.set_xlabel('Date', fontsize=12)
        ax.set_ylabel(metric_column.replace('_', ' ').title(), fontsize=12)
        ax.set_title(title or f'Business Metrics - {metric_column}', 
                     fontsize=14, fontweight='bold')
        ax.grid(True, alpha=0.3)
        ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: f'{int(x):,}'))
        
        plt.xticks(rotation=45)
        plt.tight_layout()
        plt.show()
        
        logger.info("✓ Business metrics chart created")

    def plot_trips_by_city(
        self,
        gold_table: str,
        year_column: str = "year",
        month_column: str = "month",
        city_column: str = "pickup_zone",
        trips_column: str = "trips",
        title: Optional[str] = None
    ) -> None:
        """
        Create a multiple line graph showing trips by city (zone) over time.
        
        Each city/zone is represented by a separate line, allowing comparison
        of trip volumes across different geographic areas over time.
        
        Args:
            gold_table: Gold table name containing aggregated trip data
            year_column: Column name for year
            month_column: Column name for month
            city_column: Column name for city/zone identifier
            trips_column: Column name for trip count metric
            title: Optional custom title for the chart
        """
        logger.info(f"Creating trips by city multi-line chart for {gold_table}")
        
        full_table_name = f"{self.catalog}.{self.schema}.{gold_table}"
        
        # Query to get trips by city and month
        df = self.spark.sql(f"""
            SELECT 
                {year_column},
                {month_column},
                {city_column},
                {trips_column}
            FROM {full_table_name}
            ORDER BY {year_column}, {month_column}, {city_column}
        """)
        
        pdf = df.toPandas()
        
        if pdf.empty:
            logger.warning(f"No data found in {gold_table}")
            return
        
        # Create a date column for the x-axis
        pdf['year_month'] = pd.to_datetime(
            pdf[[year_column, month_column]].assign(day=1)
        )
        
        # Create the multi-line plot
        fig, ax = plt.subplots(figsize=(12, 6))
        
        # Plot a line for each city/zone
        for city, city_data in pdf.groupby(city_column):
            city_data = city_data.sort_values('year_month')
            ax.plot(
                city_data['year_month'], 
                city_data[trips_column], 
                marker='o', 
                label=city, 
                linewidth=2, 
                markersize=4
            )
        
        ax.set_xlabel('Month', fontsize=12)
        ax.set_ylabel('Number of Trips', fontsize=12)
        ax.set_title(
            title or 'Trips by City (Zone) Over Time', 
            fontsize=14, 
            fontweight='bold'
        )
        ax.legend(title='Zone', bbox_to_anchor=(1.05, 1), loc='upper left')
        ax.grid(True, alpha=0.3)
        ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: f'{int(x):,}'))
        
        plt.xticks(rotation=45)
        plt.tight_layout()
        plt.show()
        
        logger.info("✓ Trips by city multi-line chart created")
    
    def create_dashboard_summary(
        self,
        year: str = "2023"
    ) -> None:
        """
        Create a comprehensive dashboard summary
        
        Args:
            year: Year to analyze
        """
        logger.info(f"Creating dashboard summary for {year}")
        
        print("=" * 80)
        print(f"MEDALLION ARCHITECTURE DASHBOARD - {year}")
        print("=" * 80)
        
        # Bronze layer stats
        print("\n📊 BRONZE LAYER")
        print("-" * 80)
        bronze_table = f"bronze_gm_data_{year}"
        try:
            bronze_df = self.spark.table(f"{self.catalog}.{self.schema}.{bronze_table}")
            bronze_count = bronze_df.count()
            bronze_cols = len(bronze_df.columns)
            print(f"  Table: {bronze_table}")
            print(f"  Records: {bronze_count:,}")
            print(f"  Columns: {bronze_cols}")
        except Exception as e:
            print(f"  ⚠️  Table not found or error: {e}")
        
        # Silver layer stats
        print("\n✨ SILVER LAYER")
        print("-" * 80)
        silver_table = f"silver_gm_data_{year}"
        try:
            silver_df = self.spark.table(f"{self.catalog}.{self.schema}.{silver_table}")
            silver_count = silver_df.count()
            silver_cols = len(silver_df.columns)
            
            # Data quality metrics
            if "_is_valid" in silver_df.columns:
                valid_count = silver_df.filter(col("_is_valid") == True).count()
                valid_pct = (valid_count / silver_count * 100) if silver_count > 0 else 0
            else:
                valid_pct = 100
            
            print(f"  Table: {silver_table}")
            print(f"  Records: {silver_count:,}")
            print(f"  Columns: {silver_cols}")
            print(f"  Data Quality: {valid_pct:.2f}% valid records")
        except Exception as e:
            print(f"  ⚠️  Table not found or error: {e}")
        
        # Gold layer stats
        print("\n🏆 GOLD LAYER")
        print("-" * 80)
        for agg_type in ["daily", "monthly"]:
            gold_table = f"gold_gm_data_{year}_{agg_type}"
            try:
                gold_df = self.spark.table(f"{self.catalog}.{self.schema}.{gold_table}")
                gold_count = gold_df.count()
                print(f"  Table: {gold_table}")
                print(f"  Records: {gold_count:,}")
            except Exception as e:
                print(f"  ⚠️  {gold_table} not found")
        
        print("\n" + "=" * 80)
        logger.info("✓ Dashboard summary created")


def create_visualizations(
    spark: SparkSession,
    year: str = "2023"
) -> None:
    """
    Create all visualizations for the medallion architecture
    
    Args:
        spark: SparkSession
        year: Year to analyze
    """
    logger.info(f"Creating all visualizations for {year}")
    
    viz = AnalyticsVisualizer(spark)
    
    # Dashboard summary
    viz.create_dashboard_summary(year)
    
    # Note: Update these calls with actual column names after data discovery
    
    # # Data volume trends
    # viz.plot_data_volume_trends(
    #     table_name=f"bronze_gm_data_{year}",
    #     date_column="_ingestion_timestamp"
    # )
    
    # # Data quality metrics
    # viz.plot_data_quality_metrics(
    #     table_name=f"silver_gm_data_{year}"
    # )
    
    # # Layer comparison
    # viz.plot_layer_comparison(
    #     bronze_table=f"bronze_gm_data_{year}",
    #     silver_table=f"silver_gm_data_{year}",
    #     gold_table=f"gold_gm_data_{year}_daily"
    # )
    
    # # Trips by city (zone) over time - multi-line graph
    # viz.plot_trips_by_city(
    #     gold_table=f"gold_gm_data_{year}_monthly",
    #     year_column="year",
    #     month_column="month",
    #     city_column="pickup_zone",
    #     trips_column="trips"
    # )
    
    logger.info("✓ All visualizations created")


if __name__ == "__main__":
    spark = SparkSession.builder.getOrCreate()
    create_visualizations(spark, "2023")
