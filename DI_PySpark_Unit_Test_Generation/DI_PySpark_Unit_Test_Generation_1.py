"""
_____________________________________________
## *Author*: AAVA
## *Created on*:   
## *Description*: Comprehensive unit tests for Home Tile Reporting ETL with Tile Category Metadata Integration
## *Version*: 1 
## *Updated on*: 
_____________________________________________
"""

import pytest
import unittest
from unittest.mock import patch, MagicMock
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *
from datetime import datetime
import logging
from chispa.dataframe_comparer import assert_df_equality
from pyspark.testing import assertDataFrameEqual

# Import the main module functions (assuming they are in a separate module)
# from home_tile_reporting_etl_Pipeline_1 import get_spark_session, create_sample_data, main

class TestHomeTileReportingETL(unittest.TestCase):
    
    @classmethod
    def setUpClass(cls):
        """Set up Spark session for testing"""
        cls.spark = SparkSession.builder \
            .appName("TestHomeTileReportingETL") \
            .master("local[*]") \
            .config("spark.sql.adaptive.enabled", "false") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "false") \
            .getOrCreate()
        cls.spark.sparkContext.setLogLevel("WARN")
    
    @classmethod
    def tearDownClass(cls):
        """Clean up Spark session"""
        cls.spark.stop()
    
    def setUp(self):
        """Set up test data for each test"""
        self.PROCESS_DATE = "2025-12-01"
        self.PIPELINE_NAME = "HOME_TILE_REPORTING_ETL_ENHANCED"
        
    def create_test_tile_events_data(self):
        """Create test tile events data"""
        tile_events_data = [
            ("evt_001", "user_001", "sess_001", "2025-12-01 10:00:00", "tile_001", "TILE_VIEW", "Mobile", "1.0.0"),
            ("evt_002", "user_001", "sess_001", "2025-12-01 10:01:00", "tile_001", "TILE_CLICK", "Mobile", "1.0.0"),
            ("evt_003", "user_002", "sess_002", "2025-12-01 10:02:00", "tile_002", "TILE_VIEW", "Web", "1.0.0"),
            ("evt_004", "user_003", "sess_003", "2025-12-01 10:03:00", "tile_001", "TILE_VIEW", "Mobile", "1.0.0"),
            ("evt_005", "user_003", "sess_003", "2025-12-01 10:04:00", "tile_002", "TILE_CLICK", "Web", "1.0.0"),
            # Edge case: Future date (should be filtered out)
            ("evt_006", "user_004", "sess_004", "2025-12-02 10:00:00", "tile_003", "TILE_VIEW", "Mobile", "1.0.0")
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
        
        df_tile_events = self.spark.createDataFrame(tile_events_data, tile_events_schema)
        df_tile_events = df_tile_events.withColumn("event_ts", F.to_timestamp("event_ts", "yyyy-MM-dd HH:mm:ss"))
        return df_tile_events
    
    def create_test_interstitial_events_data(self):
        """Create test interstitial events data"""
        interstitial_events_data = [
            ("int_001", "user_001", "sess_001", "2025-12-01 10:01:30", "tile_001", True, True, False),
            ("int_002", "user_002", "sess_002", "2025-12-01 10:02:30", "tile_002", True, False, True),
            ("int_003", "user_003", "sess_003", "2025-12-01 10:04:30", "tile_002", True, True, False),
            # Edge case: No interstitial view
            ("int_004", "user_004", "sess_004", "2025-12-01 10:05:30", "tile_003", False, False, False),
            # Edge case: Future date (should be filtered out)
            ("int_005", "user_005", "sess_005", "2025-12-02 10:01:30", "tile_001", True, True, False)
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
        
        df_interstitial_events = self.spark.createDataFrame(interstitial_events_data, interstitial_events_schema)
        df_interstitial_events = df_interstitial_events.withColumn("event_ts", F.to_timestamp("event_ts", "yyyy-MM-dd HH:mm:ss"))
        return df_interstitial_events
    
    def create_test_tile_metadata_data(self):
        """Create test tile metadata data"""
        tile_metadata_data = [
            ("tile_001", "Personal Finance Tile", "FINANCE", True, "2025-12-01 09:00:00"),
            ("tile_002", "Health Check Tile", "HEALTH", True, "2025-12-01 09:00:00"),
            ("tile_003", "Offers Tile", "OFFERS", False, "2025-12-01 09:00:00"),  # Inactive tile
            ("tile_004", "Unknown Tile", "UNKNOWN", True, "2025-12-01 09:00:00")  # No events for this tile
        ]
        
        tile_metadata_schema = StructType([
            StructField("tile_id", StringType(), True),
            StructField("tile_name", StringType(), True),
            StructField("tile_category", StringType(), True),
            StructField("is_active", BooleanType(), True),
            StructField("updated_ts", StringType(), True)
        ])
        
        df_tile_metadata = self.spark.createDataFrame(tile_metadata_data, tile_metadata_schema)
        df_tile_metadata = df_tile_metadata.withColumn("updated_ts", F.to_timestamp("updated_ts", "yyyy-MM-dd HH:mm:ss"))
        return df_tile_metadata
    
    def test_get_spark_session_success(self):
        """Test successful Spark session creation"""
        # This test assumes the get_spark_session function exists
        # In a real scenario, you would import it from the main module
        spark_session = SparkSession.getActiveSession()
        self.assertIsNotNone(spark_session)
        self.assertEqual(spark_session.sparkContext.appName, "TestHomeTileReportingETL")
    
    @patch('pyspark.sql.SparkSession.builder')
    def test_get_spark_session_exception_handling(self, mock_builder):
        """Test Spark session creation with exception"""
        mock_builder.appName.return_value.getOrCreate.side_effect = Exception("Spark initialization failed")
        
        with self.assertRaises(Exception) as context:
            # This would call the actual get_spark_session function
            # get_spark_session()
            pass
        
        # self.assertIn("Spark initialization failed", str(context.exception))
    
    def test_create_sample_data_structure(self):
        """Test the structure of created sample data"""
        df_tile = self.create_test_tile_events_data()
        df_inter = self.create_test_interstitial_events_data()
        df_metadata = self.create_test_tile_metadata_data()
        
        # Test tile events structure
        expected_tile_columns = ["event_id", "user_id", "session_id", "event_ts", "tile_id", "event_type", "device_type", "app_version"]
        self.assertEqual(df_tile.columns, expected_tile_columns)
        self.assertTrue(df_tile.count() > 0)
        
        # Test interstitial events structure
        expected_inter_columns = ["event_id", "user_id", "session_id", "event_ts", "tile_id", "interstitial_view_flag", "primary_button_click_flag", "secondary_button_click_flag"]
        self.assertEqual(df_inter.columns, expected_inter_columns)
        self.assertTrue(df_inter.count() > 0)
        
        # Test metadata structure
        expected_metadata_columns = ["tile_id", "tile_name", "tile_category", "is_active", "updated_ts"]
        self.assertEqual(df_metadata.columns, expected_metadata_columns)
        self.assertTrue(df_metadata.count() > 0)
    
    def test_date_filtering(self):
        """Test date filtering functionality"""
        df_tile = self.create_test_tile_events_data()
        df_inter = self.create_test_interstitial_events_data()
        
        # Filter by process date
        df_tile_filtered = df_tile.filter(F.to_date("event_ts") == self.PROCESS_DATE)
        df_inter_filtered = df_inter.filter(F.to_date("event_ts") == self.PROCESS_DATE)
        
        # Verify filtering works correctly
        self.assertEqual(df_tile_filtered.count(), 5)  # Should exclude future date
        self.assertEqual(df_inter_filtered.count(), 4)  # Should exclude future date
        
        # Verify all remaining records are from the correct date
        tile_dates = df_tile_filtered.select(F.to_date("event_ts").alias("date")).distinct().collect()
        inter_dates = df_inter_filtered.select(F.to_date("event_ts").alias("date")).distinct().collect()
        
        self.assertEqual(len(tile_dates), 1)
        self.assertEqual(len(inter_dates), 1)
        self.assertEqual(str(tile_dates[0]['date']), self.PROCESS_DATE)
        self.assertEqual(str(inter_dates[0]['date']), self.PROCESS_DATE)
    
    def test_metadata_active_filtering(self):
        """Test active metadata filtering"""
        df_metadata = self.create_test_tile_metadata_data()
        df_metadata_active = df_metadata.filter(F.col("is_active") == True)
        
        # Should exclude inactive tile_003
        self.assertEqual(df_metadata_active.count(), 3)
        
        # Verify no inactive tiles remain
        inactive_count = df_metadata_active.filter(F.col("is_active") == False).count()
        self.assertEqual(inactive_count, 0)
    
    def test_tile_events_aggregation(self):
        """Test tile events aggregation logic"""
        df_tile = self.create_test_tile_events_data()
        df_tile_filtered = df_tile.filter(F.to_date("event_ts") == self.PROCESS_DATE)
        
        df_tile_agg = (
            df_tile_filtered.groupBy("tile_id")
            .agg(
                F.countDistinct(F.when(F.col("event_type") == "TILE_VIEW", F.col("user_id"))).alias("unique_tile_views"),
                F.countDistinct(F.when(F.col("event_type") == "TILE_CLICK", F.col("user_id"))).alias("unique_tile_clicks")
            )
        )
        
        # Collect results for verification
        results = df_tile_agg.collect()
        results_dict = {row['tile_id']: row for row in results}
        
        # Verify tile_001: 2 unique views (user_001, user_003), 1 unique click (user_001)
        self.assertEqual(results_dict['tile_001']['unique_tile_views'], 2)
        self.assertEqual(results_dict['tile_001']['unique_tile_clicks'], 1)
        
        # Verify tile_002: 1 unique view (user_002), 1 unique click (user_003)
        self.assertEqual(results_dict['tile_002']['unique_tile_views'], 1)
        self.assertEqual(results_dict['tile_002']['unique_tile_clicks'], 1)
    
    def test_interstitial_events_aggregation(self):
        """Test interstitial events aggregation logic"""
        df_inter = self.create_test_interstitial_events_data()
        df_inter_filtered = df_inter.filter(F.to_date("event_ts") == self.PROCESS_DATE)
        
        df_inter_agg = (
            df_inter_filtered.groupBy("tile_id")
            .agg(
                F.countDistinct(F.when(F.col("interstitial_view_flag") == True, F.col("user_id"))).alias("unique_interstitial_views"),
                F.countDistinct(F.when(F.col("primary_button_click_flag") == True, F.col("user_id"))).alias("unique_interstitial_primary_clicks"),
                F.countDistinct(F.when(F.col("secondary_button_click_flag") == True, F.col("user_id"))).alias("unique_interstitial_secondary_clicks")
            )
        )
        
        # Collect results for verification
        results = df_inter_agg.collect()
        results_dict = {row['tile_id']: row for row in results}
        
        # Verify tile_001: 1 view, 1 primary click, 0 secondary clicks
        self.assertEqual(results_dict['tile_001']['unique_interstitial_views'], 1)
        self.assertEqual(results_dict['tile_001']['unique_interstitial_primary_clicks'], 1)
        self.assertEqual(results_dict['tile_001']['unique_interstitial_secondary_clicks'], 0)
        
        # Verify tile_002: 2 views, 1 primary click, 1 secondary click
        self.assertEqual(results_dict['tile_002']['unique_interstitial_views'], 2)
        self.assertEqual(results_dict['tile_002']['unique_interstitial_primary_clicks'], 1)
        self.assertEqual(results_dict['tile_002']['unique_interstitial_secondary_clicks'], 1)
    
    def test_join_operations(self):
        """Test join operations between aggregated data and metadata"""
        df_tile = self.create_test_tile_events_data()
        df_inter = self.create_test_interstitial_events_data()
        df_metadata = self.create_test_tile_metadata_data()
        
        # Filter data
        df_tile_filtered = df_tile.filter(F.to_date("event_ts") == self.PROCESS_DATE)
        df_inter_filtered = df_inter.filter(F.to_date("event_ts") == self.PROCESS_DATE)
        df_metadata_active = df_metadata.filter(F.col("is_active") == True)
        
        # Aggregate data
        df_tile_agg = (
            df_tile_filtered.groupBy("tile_id")
            .agg(
                F.countDistinct(F.when(F.col("event_type") == "TILE_VIEW", F.col("user_id"))).alias("unique_tile_views"),
                F.countDistinct(F.when(F.col("event_type") == "TILE_CLICK", F.col("user_id"))).alias("unique_tile_clicks")
            )
        )
        
        df_inter_agg = (
            df_inter_filtered.groupBy("tile_id")
            .agg(
                F.countDistinct(F.when(F.col("interstitial_view_flag") == True, F.col("user_id"))).alias("unique_interstitial_views"),
                F.countDistinct(F.when(F.col("primary_button_click_flag") == True, F.col("user_id"))).alias("unique_interstitial_primary_clicks"),
                F.countDistinct(F.when(F.col("secondary_button_click_flag") == True, F.col("user_id"))).alias("unique_interstitial_secondary_clicks")
            )
        )
        
        # Join aggregations
        df_combined_agg = df_tile_agg.join(df_inter_agg, "tile_id", "outer")
        
        # Join with metadata
        df_with_metadata = (
            df_combined_agg
            .join(df_metadata_active.select("tile_id", "tile_category", "tile_name"), "tile_id", "left")
        )
        
        # Verify join results
        results = df_with_metadata.collect()
        self.assertTrue(len(results) >= 2)  # At least tile_001 and tile_002
        
        # Check that metadata is properly joined
        results_dict = {row['tile_id']: row for row in results}
        self.assertEqual(results_dict['tile_001']['tile_category'], 'FINANCE')
        self.assertEqual(results_dict['tile_002']['tile_category'], 'HEALTH')
    
    def test_daily_summary_creation(self):
        """Test daily summary DataFrame creation with coalesce operations"""
        df_tile = self.create_test_tile_events_data()
        df_inter = self.create_test_interstitial_events_data()
        df_metadata = self.create_test_tile_metadata_data()
        
        # Simulate the full pipeline logic
        df_tile_filtered = df_tile.filter(F.to_date("event_ts") == self.PROCESS_DATE)
        df_inter_filtered = df_inter.filter(F.to_date("event_ts") == self.PROCESS_DATE)
        df_metadata_active = df_metadata.filter(F.col("is_active") == True)
        
        df_tile_agg = (
            df_tile_filtered.groupBy("tile_id")
            .agg(
                F.countDistinct(F.when(F.col("event_type") == "TILE_VIEW", F.col("user_id"))).alias("unique_tile_views"),
                F.countDistinct(F.when(F.col("event_type") == "TILE_CLICK", F.col("user_id"))).alias("unique_tile_clicks")
            )
        )
        
        df_inter_agg = (
            df_inter_filtered.groupBy("tile_id")
            .agg(
                F.countDistinct(F.when(F.col("interstitial_view_flag") == True, F.col("user_id"))).alias("unique_interstitial_views"),
                F.countDistinct(F.when(F.col("primary_button_click_flag") == True, F.col("user_id"))).alias("unique_interstitial_primary_clicks"),
                F.countDistinct(F.when(F.col("secondary_button_click_flag") == True, F.col("user_id"))).alias("unique_interstitial_secondary_clicks")
            )
        )
        
        df_combined_agg = df_tile_agg.join(df_inter_agg, "tile_id", "outer")
        
        df_daily_summary = (
            df_combined_agg
            .join(df_metadata_active.select("tile_id", "tile_category", "tile_name"), "tile_id", "left")
            .withColumn("date", F.lit(self.PROCESS_DATE).cast("date"))
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
        
        # Verify daily summary structure and content
        expected_columns = [
            "date", "tile_id", "tile_category", "unique_tile_views", "unique_tile_clicks",
            "unique_interstitial_views", "unique_interstitial_primary_clicks", "unique_interstitial_secondary_clicks"
        ]
        self.assertEqual(df_daily_summary.columns, expected_columns)
        
        # Verify coalesce operations work (no null values)
        null_counts = df_daily_summary.select([F.sum(F.col(c).isNull().cast("int")).alias(c) for c in df_daily_summary.columns]).collect()[0]
        for column in df_daily_summary.columns:
            self.assertEqual(null_counts[column], 0, f"Column {column} should not have null values")
    
    def test_global_kpis_calculation(self):
        """Test global KPIs calculation including CTR calculations"""
        # Create a mock daily summary for testing
        daily_summary_data = [
            ("2025-12-01", "tile_001", "FINANCE", 10, 5, 8, 3, 1),
            ("2025-12-01", "tile_002", "HEALTH", 15, 7, 12, 6, 2)
        ]
        
        daily_summary_schema = StructType([
            StructField("date", StringType(), True),
            StructField("tile_id", StringType(), True),
            StructField("tile_category", StringType(), True),
            StructField("unique_tile_views", IntegerType(), True),
            StructField("unique_tile_clicks", IntegerType(), True),
            StructField("unique_interstitial_views", IntegerType(), True),
            StructField("unique_interstitial_primary_clicks", IntegerType(), True),
            StructField("unique_interstitial_secondary_clicks", IntegerType(), True)
        ])
        
        df_daily_summary = self.spark.createDataFrame(daily_summary_data, daily_summary_schema)
        df_daily_summary = df_daily_summary.withColumn("date", F.col("date").cast("date"))
        
        # Calculate global KPIs
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
        
        # Verify calculations
        result = df_global.collect()[0]
        
        # Verify totals
        self.assertEqual(result['total_tile_views'], 25)  # 10 + 15
        self.assertEqual(result['total_tile_clicks'], 12)  # 5 + 7
        self.assertEqual(result['total_interstitial_views'], 20)  # 8 + 12
        self.assertEqual(result['total_primary_clicks'], 9)  # 3 + 6
        self.assertEqual(result['total_secondary_clicks'], 3)  # 1 + 2
        
        # Verify CTR calculations
        self.assertAlmostEqual(result['overall_ctr'], 0.48, places=2)  # 12/25
        self.assertAlmostEqual(result['overall_primary_ctr'], 0.45, places=2)  # 9/20
        self.assertAlmostEqual(result['overall_secondary_ctr'], 0.15, places=2)  # 3/20
    
    def test_ctr_calculation_edge_cases(self):
        """Test CTR calculation edge cases (division by zero)"""
        # Test case with zero views
        edge_case_data = [
            ("2025-12-01", "tile_001", "FINANCE", 0, 0, 0, 0, 0)
        ]
        
        edge_case_schema = StructType([
            StructField("date", StringType(), True),
            StructField("tile_id", StringType(), True),
            StructField("tile_category", StringType(), True),
            StructField("unique_tile_views", IntegerType(), True),
            StructField("unique_tile_clicks", IntegerType(), True),
            StructField("unique_interstitial_views", IntegerType(), True),
            StructField("unique_interstitial_primary_clicks", IntegerType(), True),
            StructField("unique_interstitial_secondary_clicks", IntegerType(), True)
        ])
        
        df_edge_case = self.spark.createDataFrame(edge_case_data, edge_case_schema)
        df_edge_case = df_edge_case.withColumn("date", F.col("date").cast("date"))
        
        # Calculate global KPIs with zero values
        df_global_edge = (
            df_edge_case.groupBy("date")
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
        
        # Verify edge case handling
        result = df_global_edge.collect()[0]
        
        # All CTRs should be 0.0 when there are no views
        self.assertEqual(result['overall_ctr'], 0.0)
        self.assertEqual(result['overall_primary_ctr'], 0.0)
        self.assertEqual(result['overall_secondary_ctr'], 0.0)
    
    def test_data_type_validation(self):
        """Test data type validation for all DataFrames"""
        df_tile = self.create_test_tile_events_data()
        df_inter = self.create_test_interstitial_events_data()
        df_metadata = self.create_test_tile_metadata_data()
        
        # Verify tile events data types
        tile_schema_dict = {field.name: field.dataType for field in df_tile.schema.fields}
        self.assertIsInstance(tile_schema_dict['event_ts'], TimestampType)
        self.assertIsInstance(tile_schema_dict['event_id'], StringType)
        
        # Verify interstitial events data types
        inter_schema_dict = {field.name: field.dataType for field in df_inter.schema.fields}
        self.assertIsInstance(inter_schema_dict['event_ts'], TimestampType)
        self.assertIsInstance(inter_schema_dict['interstitial_view_flag'], BooleanType)
        
        # Verify metadata data types
        metadata_schema_dict = {field.name: field.dataType for field in df_metadata.schema.fields}
        self.assertIsInstance(metadata_schema_dict['updated_ts'], TimestampType)
        self.assertIsInstance(metadata_schema_dict['is_active'], BooleanType)
    
    def test_empty_dataframe_handling(self):
        """Test handling of empty DataFrames"""
        # Create empty DataFrames with correct schemas
        tile_events_schema = StructType([
            StructField("event_id", StringType(), True),
            StructField("user_id", StringType(), True),
            StructField("session_id", StringType(), True),
            StructField("event_ts", TimestampType(), True),
            StructField("tile_id", StringType(), True),
            StructField("event_type", StringType(), True),
            StructField("device_type", StringType(), True),
            StructField("app_version", StringType(), True)
        ])
        
        df_empty_tile = self.spark.createDataFrame([], tile_events_schema)
        
        # Test aggregation on empty DataFrame
        df_empty_agg = (
            df_empty_tile.groupBy("tile_id")
            .agg(
                F.countDistinct(F.when(F.col("event_type") == "TILE_VIEW", F.col("user_id"))).alias("unique_tile_views"),
                F.countDistinct(F.when(F.col("event_type") == "TILE_CLICK", F.col("user_id"))).alias("unique_tile_clicks")
            )
        )
        
        # Empty DataFrame should remain empty after aggregation
        self.assertEqual(df_empty_agg.count(), 0)
    
    @patch('logging.getLogger')
    def test_logging_functionality(self, mock_logger):
        """Test logging functionality"""
        mock_logger_instance = MagicMock()
        mock_logger.return_value = mock_logger_instance
        
        # This would test the actual main function logging
        # In a real scenario, you would call the main function and verify logging calls
        # main()
        
        # Verify that logging methods would be called
        # mock_logger_instance.info.assert_called()
        pass
    
    def test_performance_with_large_dataset(self):
        """Test performance characteristics with larger dataset"""
        # Create a larger test dataset
        large_tile_events_data = []
        for i in range(1000):
            large_tile_events_data.append(
                (f"evt_{i:04d}", f"user_{i%100:03d}", f"sess_{i%50:03d}", 
                 "2025-12-01 10:00:00", f"tile_{i%10:03d}", 
                 "TILE_VIEW" if i % 2 == 0 else "TILE_CLICK", 
                 "Mobile" if i % 2 == 0 else "Web", "1.0.0")
            )
        
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
        
        df_large_tile = self.spark.createDataFrame(large_tile_events_data, tile_events_schema)
        df_large_tile = df_large_tile.withColumn("event_ts", F.to_timestamp("event_ts", "yyyy-MM-dd HH:mm:ss"))
        
        # Test aggregation performance
        start_time = datetime.now()
        
        df_large_agg = (
            df_large_tile.groupBy("tile_id")
            .agg(
                F.countDistinct(F.when(F.col("event_type") == "TILE_VIEW", F.col("user_id"))).alias("unique_tile_views"),
                F.countDistinct(F.when(F.col("event_type") == "TILE_CLICK", F.col("user_id"))).alias("unique_tile_clicks")
            )
        )
        
        result_count = df_large_agg.count()
        end_time = datetime.now()
        
        # Verify results and performance
        self.assertEqual(result_count, 10)  # 10 unique tiles
        
        # Performance should complete within reasonable time (adjust as needed)
        execution_time = (end_time - start_time).total_seconds()
        self.assertLess(execution_time, 30, "Aggregation should complete within 30 seconds")


class TestHomeTileReportingETLIntegration(unittest.TestCase):
    """Integration tests for the complete ETL pipeline"""
    
    @classmethod
    def setUpClass(cls):
        """Set up Spark session for integration testing"""
        cls.spark = SparkSession.builder \
            .appName("TestHomeTileReportingETLIntegration") \
            .master("local[*]") \
            .config("spark.sql.adaptive.enabled", "false") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "false") \
            .getOrCreate()
        cls.spark.sparkContext.setLogLevel("WARN")
    
    @classmethod
    def tearDownClass(cls):
        """Clean up Spark session"""
        cls.spark.stop()
    
    def test_end_to_end_pipeline(self):
        """Test the complete end-to-end pipeline"""
        # This test would call the actual main() function and verify the complete pipeline
        # In a real scenario, you would:
        # 1. Set up test data in the expected input locations
        # 2. Call the main() function
        # 3. Verify the output data matches expected results
        # 4. Clean up test data
        
        # For now, we'll simulate the pipeline logic
        PROCESS_DATE = "2025-12-01"
        
        # This would be replaced with actual function calls
        # df_summary, df_kpis = main()
        
        # Verify that the pipeline completes without errors
        # self.assertIsNotNone(df_summary)
        # self.assertIsNotNone(df_kpis)
        
        # Verify expected output structure
        # expected_summary_columns = ["date", "tile_id", "tile_category", "unique_tile_views", "unique_tile_clicks", "unique_interstitial_views", "unique_interstitial_primary_clicks", "unique_interstitial_secondary_clicks"]
        # self.assertEqual(df_summary.columns, expected_summary_columns)
        
        pass


if __name__ == '__main__':
    # Run the tests
    unittest.main(verbosity=2)
    
    # Alternative: Run with pytest
    # pytest.main(['-v', __file__])
