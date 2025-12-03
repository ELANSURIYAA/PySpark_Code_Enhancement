"""
_____________________________________________
## *Author*: AAVA
## *Created on*:   
## *Description*: Enhanced Home Tile Reporting ETL with Tile Category Metadata Integration
## *Version*: 1 
## *Updated on*: 
_____________________________________________
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *
from datetime import datetime
import logging

# Configuration
PIPELINE_NAME = "HOME_TILE_REPORTING_ETL_ENHANCED"
PROCESS_DATE = "2025-12-01"

# Initialize Spark Session
def get_spark_session():
    try:
        spark = SparkSession.getActiveSession()
        if spark is None:
            spark = SparkSession.builder.appName("HomeTileReportingETLEnhanced").getOrCreate()
        return spark
    except Exception as e:
        print(f"Error initializing Spark session: {e}")
        raise

spark = get_spark_session()
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Create sample data for testing
def create_sample_data():
    # Sample Home Tile Events
    tile_events_data = [
        ("evt_001", "user_001", "sess_001", "2025-12-01 10:00:00", "tile_001", "TILE_VIEW", "Mobile", "1.0.0"),
        ("evt_002", "user_001", "sess_001", "2025-12-01 10:01:00", "tile_001", "TILE_CLICK", "Mobile", "1.0.0"),
        ("evt_003", "user_002", "sess_002", "2025-12-01 10:02:00", "tile_002", "TILE_VIEW", "Web", "1.0.0"),
        ("evt_004", "user_003", "sess_003", "2025-12-01 10:03:00", "tile_001", "TILE_VIEW", "Mobile", "1.0.0"),
        ("evt_005", "user_003", "sess_003", "2025-12-01 10:04:00", "tile_002", "TILE_CLICK", "Web", "1.0.0")
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
    
    # Sample Interstitial Events
    interstitial_events_data = [
        ("int_001", "user_001", "sess_001", "2025-12-01 10:01:30", "tile_001", True, True, False),
        ("int_002", "user_002", "sess_002", "2025-12-01 10:02:30", "tile_002", True, False, True),
        ("int_003", "user_003", "sess_003", "2025-12-01 10:04:30", "tile_002", True, True, False)
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
    
    # Sample Tile Metadata
    tile_metadata_data = [
        ("tile_001", "Personal Finance Tile", "FINANCE", True, "2025-12-01 09:00:00"),
        ("tile_002", "Health Check Tile", "HEALTH", True, "2025-12-01 09:00:00"),
        ("tile_003", "Offers Tile", "OFFERS", True, "2025-12-01 09:00:00")
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

# Main ETL Logic
def main():
    try:
        logger.info(f"Starting {PIPELINE_NAME} for date: {PROCESS_DATE}")
        
        # Read source data
        df_tile, df_inter, df_metadata = create_sample_data()
        df_tile = df_tile.filter(F.to_date("event_ts") == PROCESS_DATE)
        df_inter = df_inter.filter(F.to_date("event_ts") == PROCESS_DATE)
        df_metadata = df_metadata.filter(F.col("is_active") == True)
        
        # Aggregate tile events
        df_tile_agg = (
            df_tile.groupBy("tile_id")
            .agg(
                F.countDistinct(F.when(F.col("event_type") == "TILE_VIEW", F.col("user_id"))).alias("unique_tile_views"),
                F.countDistinct(F.when(F.col("event_type") == "TILE_CLICK", F.col("user_id"))).alias("unique_tile_clicks")
            )
        )
        
        # Aggregate interstitial events
        df_inter_agg = (
            df_inter.groupBy("tile_id")
            .agg(
                F.countDistinct(F.when(F.col("interstitial_view_flag") == True, F.col("user_id"))).alias("unique_interstitial_views"),
                F.countDistinct(F.when(F.col("primary_button_click_flag") == True, F.col("user_id"))).alias("unique_interstitial_primary_clicks"),
                F.countDistinct(F.when(F.col("secondary_button_click_flag") == True, F.col("user_id"))).alias("unique_interstitial_secondary_clicks")
            )
        )
        
        # Join aggregations and enrich with metadata
        df_combined_agg = df_tile_agg.join(df_inter_agg, "tile_id", "outer")
        
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
        )
        
        # Global KPIs
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
                       F.col("total_tile_clicks") / F.col("total_tile_views")).otherwise(0.0)
            )
            .withColumn(
                "overall_primary_ctr",
                F.when(F.col("total_interstitial_views") > 0,
                       F.col("total_primary_clicks") / F.col("total_interstitial_views")).otherwise(0.0)
            )
            .withColumn(
                "overall_secondary_ctr",
                F.when(F.col("total_interstitial_views") > 0,
                       F.col("total_secondary_clicks") / F.col("total_interstitial_views")).otherwise(0.0)
            )
        )
        
        # Display results
        print("Daily Summary Results:")
        df_daily_summary.show(truncate=False)
        
        print("Global KPIs Results:")
        df_global.show(truncate=False)
        
        logger.info(f"ETL completed successfully for {PROCESS_DATE}")
        return df_daily_summary, df_global
        
    except Exception as e:
        logger.error(f"ETL failed: {e}")
        raise

if __name__ == "__main__":
    df_summary, df_kpis = main()
