"""
_____________________________________________
## *Author*: AAVA
## *Created on*:   
## *Description*: Enhanced Home Tile Reporting ETL with Advanced Tile Category Analytics and Performance Optimization
## *Version*: 2 
## *Updated on*: 
## *Changes*: Added category-level CTR calculations, performance optimizations, enhanced error handling, and comprehensive data validation
## *Reason*: Business requirement for category-level analytics and improved pipeline reliability as per PCE-3
_____________________________________________
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *
from datetime import datetime
import logging

# Configuration
PIPELINE_NAME = "HOME_TILE_REPORTING_ETL_ENHANCED_V2"
PROCESS_DATE = "2025-12-01"

# Initialize Spark Session with optimizations
def get_spark_session():
    try:
        spark = SparkSession.getActiveSession()
        if spark is None:
            spark = (SparkSession.builder
                    .appName("HomeTileReportingETLEnhancedV2")
                    .config("spark.sql.adaptive.enabled", "true")
                    .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
                    .config("spark.sql.adaptive.skewJoin.enabled", "true")
                    .getOrCreate())
        return spark
    except Exception as e:
        print(f"Error initializing Spark session: {e}")
        raise

spark = get_spark_session()
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Data validation functions
def validate_data_quality(df, table_name):
    """Validate data quality and log metrics"""
    try:
        record_count = df.count()
        null_counts = {col: df.filter(F.col(col).isNull()).count() for col in df.columns}
        
        logger.info(f"{table_name} - Record count: {record_count}")
        logger.info(f"{table_name} - Null counts: {null_counts}")
        
        # Check for critical nulls
        if table_name == "tile_events" and null_counts.get('tile_id', 0) > 0:
            logger.warning(f"Found {null_counts['tile_id']} null tile_ids in {table_name}")
        
        return record_count > 0
    except Exception as e:
        logger.error(f"Data validation failed for {table_name}: {e}")
        return False

# Create enhanced sample data for testing
def create_sample_data():
    # Enhanced Home Tile Events with more diverse data
    tile_events_data = [
        ("evt_001", "user_001", "sess_001", "2025-12-01 10:00:00", "tile_001", "TILE_VIEW", "Mobile", "1.0.0"),
        ("evt_002", "user_001", "sess_001", "2025-12-01 10:01:00", "tile_001", "TILE_CLICK", "Mobile", "1.0.0"),
        ("evt_003", "user_002", "sess_002", "2025-12-01 10:02:00", "tile_002", "TILE_VIEW", "Web", "1.0.0"),
        ("evt_004", "user_003", "sess_003", "2025-12-01 10:03:00", "tile_001", "TILE_VIEW", "Mobile", "1.0.0"),
        ("evt_005", "user_003", "sess_003", "2025-12-01 10:04:00", "tile_002", "TILE_CLICK", "Web", "1.0.0"),
        ("evt_006", "user_004", "sess_004", "2025-12-01 10:05:00", "tile_003", "TILE_VIEW", "Mobile", "1.0.0"),
        ("evt_007", "user_004", "sess_004", "2025-12-01 10:06:00", "tile_003", "TILE_CLICK", "Mobile", "1.0.0"),
        ("evt_008", "user_005", "sess_005", "2025-12-01 10:07:00", "tile_004", "TILE_VIEW", "Web", "1.0.0")
    ]
    
    tile_events_schema = StructType([
        StructField("event_id", StringType(), True),
        StructField("user_id", StringType(), True),
        StructField("session_id", StringType(), True),
        StructField("event_ts", StringType(), True),
        StructField("tile_id", StringType(), True),
        StructField("event_type", StringType(), True),
        StructField("device_type", StringType(), True),
        StructField("app_version", StringType(), True)
    ])
    
    df_tile_events = spark.createDataFrame(tile_events_data, tile_events_schema)
    df_tile_events = df_tile_events.withColumn("event_ts", F.to_timestamp("event_ts", "yyyy-MM-dd HH:mm:ss"))
    
    # Enhanced Interstitial Events
    interstitial_events_data = [
        ("int_001", "user_001", "sess_001", "2025-12-01 10:01:30", "tile_001", True, True, False),
        ("int_002", "user_002", "sess_002", "2025-12-01 10:02:30", "tile_002", True, False, True),
        ("int_003", "user_003", "sess_003", "2025-12-01 10:04:30", "tile_002", True, True, False),
        ("int_004", "user_004", "sess_004", "2025-12-01 10:06:30", "tile_003", True, True, True),
        ("int_005", "user_005", "sess_005", "2025-12-01 10:07:30", "tile_004", True, False, False)
    ]
    
    interstitial_events_schema = StructType([
        StructField("event_id", StringType(), True),
        StructField("user_id", StringType(), True),
        StructField("session_id", StringType(), True),
        StructField("event_ts", StringType(), True),
        StructField("tile_id", StringType(), True),
        StructField("interstitial_view_flag", BooleanType(), True),
        StructField("primary_button_click_flag", BooleanType(), True),
        StructField("secondary_button_click_flag", BooleanType(), True)
    ])
    
    df_interstitial_events = spark.createDataFrame(interstitial_events_data, interstitial_events_schema)
    df_interstitial_events = df_interstitial_events.withColumn("event_ts", F.to_timestamp("event_ts", "yyyy-MM-dd HH:mm:ss"))
    
    # Enhanced Tile Metadata with more categories
    tile_metadata_data = [
        ("tile_001", "Personal Finance Tile", "FINANCE", True, "2025-12-01 09:00:00"),
        ("tile_002", "Health Check Tile", "HEALTH", True, "2025-12-01 09:00:00"),
        ("tile_003", "Special Offers Tile", "OFFERS", True, "2025-12-01 09:00:00"),
        ("tile_004", "Payment Services Tile", "PAYMENTS", True, "2025-12-01 09:00:00"),
        ("tile_005", "Legacy Tile", "DEPRECATED", False, "2025-12-01 09:00:00")
    ]
    
    tile_metadata_schema = StructType([
        StructField("tile_id", StringType(), True),
        StructField("tile_name", StringType(), True),
        StructField("tile_category", StringType(), True),
        StructField("is_active", BooleanType(), True),
        StructField("updated_ts", StringType(), True)
    ])
    
    df_tile_metadata = spark.createDataFrame(tile_metadata_data, tile_metadata_schema)
    df_tile_metadata = df_tile_metadata.withColumn("updated_ts", F.to_timestamp("updated_ts", "yyyy-MM-dd HH:mm:ss"))
    
    return df_tile_events, df_interstitial_events, df_tile_metadata

# Enhanced aggregation functions
def calculate_tile_aggregations(df_tile):
    """Calculate tile-level aggregations with enhanced metrics"""
    return (
        df_tile.groupBy("tile_id")
        .agg(
            F.countDistinct(F.when(F.col("event_type") == "TILE_VIEW", F.col("user_id"))).alias("unique_tile_views"),
            F.countDistinct(F.when(F.col("event_type") == "TILE_CLICK", F.col("user_id"))).alias("unique_tile_clicks"),
            F.count(F.when(F.col("event_type") == "TILE_VIEW", F.col("event_id"))).alias("total_tile_views"),
            F.count(F.when(F.col("event_type") == "TILE_CLICK", F.col("event_id"))).alias("total_tile_clicks")
        )
    )

def calculate_interstitial_aggregations(df_inter):
    """Calculate interstitial-level aggregations"""
    return (
        df_inter.groupBy("tile_id")
        .agg(
            F.countDistinct(F.when(F.col("interstitial_view_flag") == True, F.col("user_id"))).alias("unique_interstitial_views"),
            F.countDistinct(F.when(F.col("primary_button_click_flag") == True, F.col("user_id"))).alias("unique_interstitial_primary_clicks"),
            F.countDistinct(F.when(F.col("secondary_button_click_flag") == True, F.col("user_id"))).alias("unique_interstitial_secondary_clicks")
        )
    )

# Main ETL Logic with enhanced features
def main():
    try:
        logger.info(f"Starting {PIPELINE_NAME} for date: {PROCESS_DATE}")
        
        # Read source data
        df_tile, df_inter, df_metadata = create_sample_data()
        
        # Data validation
        if not validate_data_quality(df_tile, "tile_events"):
            raise ValueError("Tile events data validation failed")
        if not validate_data_quality(df_inter, "interstitial_events"):
            raise ValueError("Interstitial events data validation failed")
        if not validate_data_quality(df_metadata, "tile_metadata"):
            raise ValueError("Tile metadata validation failed")
        
        # Filter data for process date and active tiles
        df_tile = df_tile.filter(F.to_date("event_ts") == PROCESS_DATE)
        df_inter = df_inter.filter(F.to_date("event_ts") == PROCESS_DATE)
        df_metadata = df_metadata.filter(F.col("is_active") == True)
        
        logger.info(f"Filtered data - Tile events: {df_tile.count()}, Interstitial events: {df_inter.count()}, Metadata: {df_metadata.count()}")
        
        # Calculate aggregations
        df_tile_agg = calculate_tile_aggregations(df_tile)
        df_inter_agg = calculate_interstitial_aggregations(df_inter)
        
        # Join aggregations and enrich with metadata
        df_combined_agg = df_tile_agg.join(df_inter_agg, "tile_id", "outer")
        
        # Enhanced daily summary with CTR calculations
        df_daily_summary = (
            df_combined_agg
            .join(df_metadata.select("tile_id", "tile_category", "tile_name"), "tile_id", "left")
            .withColumn("date", F.lit(PROCESS_DATE).cast("date"))
            .select(
                "date",
                "tile_id",
                F.coalesce("tile_category", F.lit("UNKNOWN")).alias("tile_category"),
                F.coalesce("unique_tile_views", F.lit(0)).alias("unique_tile_views"),
                F.coalesce("unique_tile_clicks", F.lit(0)).alias("unique_tile_clicks"),
                F.coalesce("unique_interstitial_views", F.lit(0)).alias("unique_interstitial_views"),
                F.coalesce("unique_interstitial_primary_clicks", F.lit(0)).alias("unique_interstitial_primary_clicks"),
                F.coalesce("unique_interstitial_secondary_clicks", F.lit(0)).alias("unique_interstitial_secondary_clicks")
            )
            .withColumn(
                "tile_ctr",
                F.when(F.col("unique_tile_views") > 0,
                       F.round(F.col("unique_tile_clicks") / F.col("unique_tile_views"), 4)).otherwise(0.0)
            )
            .withColumn(
                "primary_ctr",
                F.when(F.col("unique_interstitial_views") > 0,
                       F.round(F.col("unique_interstitial_primary_clicks") / F.col("unique_interstitial_views"), 4)).otherwise(0.0)
            )
            .withColumn(
                "secondary_ctr",
                F.when(F.col("unique_interstitial_views") > 0,
                       F.round(F.col("unique_interstitial_secondary_clicks") / F.col("unique_interstitial_views"), 4)).otherwise(0.0)
            )
        )
        
        # Enhanced Global KPIs
        df_global = (
            df_daily_summary.groupBy("date")
            .agg(
                F.sum("unique_tile_views").alias("total_tile_views"),
                F.sum("unique_tile_clicks").alias("total_tile_clicks"),
                F.sum("unique_interstitial_views").alias("total_interstitial_views"),
                F.sum("unique_interstitial_primary_clicks").alias("total_primary_clicks"),
                F.sum("unique_interstitial_secondary_clicks").alias("total_secondary_clicks")
            )
            .withColumn(
                "overall_ctr",
                F.when(F.col("total_tile_views") > 0,
                       F.round(F.col("total_tile_clicks") / F.col("total_tile_views"), 4)).otherwise(0.0)
            )
            .withColumn(
                "overall_primary_ctr",
                F.when(F.col("total_interstitial_views") > 0,
                       F.round(F.col("total_primary_clicks") / F.col("total_interstitial_views"), 4)).otherwise(0.0)
            )
            .withColumn(
                "overall_secondary_ctr",
                F.when(F.col("total_interstitial_views") > 0,
                       F.round(F.col("total_secondary_clicks") / F.col("total_interstitial_views"), 4)).otherwise(0.0)
            )
        )
        
        # NEW FEATURE: Category-level KPIs as requested in PCE-3
        df_category_kpis = (
            df_daily_summary.groupBy("date", "tile_category")
            .agg(
                F.sum("unique_tile_views").alias("category_tile_views"),
                F.sum("unique_tile_clicks").alias("category_tile_clicks"),
                F.sum("unique_interstitial_views").alias("category_interstitial_views"),
                F.sum("unique_interstitial_primary_clicks").alias("category_primary_clicks"),
                F.sum("unique_interstitial_secondary_clicks").alias("category_secondary_clicks"),
                F.count("tile_id").alias("tiles_in_category")
            )
            .withColumn(
                "category_ctr",
                F.when(F.col("category_tile_views") > 0,
                       F.round(F.col("category_tile_clicks") / F.col("category_tile_views"), 4)).otherwise(0.0)
            )
            .withColumn(
                "category_primary_ctr",
                F.when(F.col("category_interstitial_views") > 0,
                       F.round(F.col("category_primary_clicks") / F.col("category_interstitial_views"), 4)).otherwise(0.0)
            )
            .withColumn(
                "category_secondary_ctr",
                F.when(F.col("category_interstitial_views") > 0,
                       F.round(F.col("category_secondary_clicks") / F.col("category_interstitial_views"), 4)).otherwise(0.0)
            )
        )
        
        # Display results
        print("\n" + "="*80)
        print("DAILY SUMMARY RESULTS (Enhanced with CTRs)")
        print("="*80)
        df_daily_summary.show(truncate=False)
        
        print("\n" + "="*80)
        print("GLOBAL KPIs RESULTS")
        print("="*80)
        df_global.show(truncate=False)
        
        print("\n" + "="*80)
        print("CATEGORY-LEVEL KPIs RESULTS (NEW FEATURE)")
        print("="*80)
        df_category_kpis.show(truncate=False)
        
        # Final validation
        final_record_count = df_daily_summary.count()
        category_count = df_category_kpis.count()
        
        logger.info(f"ETL completed successfully for {PROCESS_DATE}")
        logger.info(f"Final record counts - Daily Summary: {final_record_count}, Category KPIs: {category_count}")
        
        return df_daily_summary, df_global, df_category_kpis
        
    except Exception as e:
        logger.error(f"ETL failed: {e}")
        raise

if __name__ == "__main__":
    df_summary, df_kpis, df_category_kpis = main()
