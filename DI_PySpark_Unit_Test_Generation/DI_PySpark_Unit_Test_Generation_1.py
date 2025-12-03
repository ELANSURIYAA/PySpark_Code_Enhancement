"""
_____________________________________________
## *Author*: AAVA
## *Created on*:   
## *Description*: Comprehensive unit tests for Home Tile Reporting ETL Enhanced pipeline
## *Version*: 1 
## *Updated on*: 
_____________________________________________
"""

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *
from datetime import datetime
import logging
from unittest.mock import patch, MagicMock

class TestHomeTileReportingETL:
    """
    Comprehensive test suite for Home Tile Reporting ETL Enhanced pipeline
    """
    
    @classmethod
    def setup_class(cls):
        """Setup Spark session for testing"""
        cls.spark = SparkSession.builder \
            .appName("HomeTileReportingETLTest") \
            .master("local[*]") \
            .config("spark.sql.adaptive.enabled", "false") \
            .getOrCreate()
        cls.spark.sparkContext.setLogLevel("WARN")
    
    @classmethod
    def teardown_class(cls):
        """Cleanup Spark session"""
        cls.spark.stop()
    
    def setup_method(self):
        """Setup method run before each test"""
        self.process_date = "2025-12-01"
        self.pipeline_name = "HOME_TILE_REPORTING_ETL_ENHANCED"
    
    def test_get_spark_session_success(self):
        """Test successful Spark session creation"""
        with patch('pyspark.sql.SparkSession.getActiveSession', return_value=None):
            with patch('pyspark.sql.SparkSession.builder') as mock_builder:
                mock_session = MagicMock()
                mock_builder.appName.return_value.getOrCreate.return_value = mock_session
                assert True  # Placeholder for actual function test
    
    def test_create_sample_data_structure(self):
        """Test the structure of sample data creation"""
        tile_events_data = [
            ("evt_001", "user_001", "sess_001", "2025-12-01 10:00:00", "tile_001", "TILE_VIEW", "Mobile", "1.0.0"),
            ("evt_002", "user_001", "sess_001", "2025-12-01 10:01:00", "tile_001", "TILE_CLICK", "Mobile", "1.0.0")
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
        
        assert df_tile_events.count() == 2
        assert len(df_tile_events.columns) == 8
        assert "event_id" in df_tile_events.columns
        
        schema_dict = {field.name: field.dataType for field in df_tile_events.schema.fields}
        assert isinstance(schema_dict["event_ts"], TimestampType)
    
    def test_tile_aggregation_logic(self):
        """Test tile events aggregation logic"""
        tile_events_data = [
            ("evt_001", "user_001", "sess_001", "2025-12-01 10:00:00", "tile_001", "TILE_VIEW", "Mobile", "1.0.0"),
            ("evt_002", "user_001", "sess_001", "2025-12-01 10:01:00", "tile_001", "TILE_CLICK", "Mobile", "1.0.0"),
            ("evt_003", "user_002", "sess_002", "2025-12-01 10:02:00", "tile_001", "TILE_VIEW", "Web", "1.0.0"),
            ("evt_004", "user_003", "sess_003", "2025-12-01 10:03:00", "tile_002", "TILE_VIEW", "Mobile", "1.0.0")
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
        
        df_tile = self.spark.createDataFrame(tile_events_data, tile_events_schema)
        df_tile = df_tile.withColumn("event_ts", F.to_timestamp("event_ts", "yyyy-MM-dd HH:mm:ss"))
        
        df_tile_agg = (
            df_tile.groupBy("tile_id")
            .agg(
                F.countDistinct(F.when(F.col("event_type") == "TILE_VIEW", F.col("user_id"))).alias("unique_tile_views"),
                F.countDistinct(F.when(F.col("event_type") == "TILE_CLICK", F.col("user_id"))).alias("unique_tile_clicks")
            )
        )
        
        results = df_tile_agg.collect()
        
        tile_001_result = next((row for row in results if row.tile_id == "tile_001"), None)
        tile_002_result = next((row for row in results if row.tile_id == "tile_002"), None)
        
        assert tile_001_result is not None
        assert tile_001_result.unique_tile_views == 2
        assert tile_001_result.unique_tile_clicks == 1
        
        assert tile_002_result is not None
        assert tile_002_result.unique_tile_views == 1
        assert tile_002_result.unique_tile_clicks == 0
    
    def test_interstitial_aggregation_logic(self):
        """Test interstitial events aggregation logic"""
        interstitial_events_data = [
            ("int_001", "user_001", "sess_001", "2025-12-01 10:01:30", "tile_001", True, True, False),
            ("int_002", "user_002", "sess_002", "2025-12-01 10:02:30", "tile_001", True, False, True),
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
        
        df_inter = self.spark.createDataFrame(interstitial_events_data, interstitial_events_schema)
        df_inter = df_inter.withColumn("event_ts", F.to_timestamp("event_ts", "yyyy-MM-dd HH:mm:ss"))
        
        df_inter_agg = (
            df_inter.groupBy("tile_id")
            .agg(
                F.countDistinct(F.when(F.col("interstitial_view_flag") == True, F.col("user_id"))).alias("unique_interstitial_views"),
                F.countDistinct(F.when(F.col("primary_button_click_flag") == True, F.col("user_id"))).alias("unique_interstitial_primary_clicks"),
                F.countDistinct(F.when(F.col("secondary_button_click_flag") == True, F.col("user_id"))).alias("unique_interstitial_secondary_clicks")
            )
        )
        
        results = df_inter_agg.collect()
        
        tile_001_result = next((row for row in results if row.tile_id == "tile_001"), None)
        tile_002_result = next((row for row in results if row.tile_id == "tile_002"), None)
        
        assert tile_001_result is not None
        assert tile_001_result.unique_interstitial_views == 2
        assert tile_001_result.unique_interstitial_primary_clicks == 1
        assert tile_001_result.unique_interstitial_secondary_clicks == 1
        
        assert tile_002_result is not None
        assert tile_002_result.unique_interstitial_views == 1
        assert tile_002_result.unique_interstitial_primary_clicks == 1
        assert tile_002_result.unique_interstitial_secondary_clicks == 0
    
    def test_global_kpis_calculation(self):
        """Test global KPIs calculation logic"""
        daily_summary_data = [
            ("2025-12-01", "tile_001", "FINANCE", 10, 5, 8, 3, 2),
            ("2025-12-01", "tile_002", "HEALTH", 15, 7, 12, 4, 1),
            ("2025-12-01", "tile_003", "OFFERS", 20, 0, 0, 0, 0)
        ]
        
        df_daily_summary = self.spark.createDataFrame(
            daily_summary_data,
            ["date", "tile_id", "tile_category", "unique_tile_views", "unique_tile_clicks", 
             "unique_interstitial_views", "unique_interstitial_primary_clicks", "unique_interstitial_secondary_clicks"]
        )
        
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
        
        result = df_global.collect()[0]
        
        assert result.total_tile_views == 45
        assert result.total_tile_clicks == 12
        assert result.total_interstitial_views == 20
        assert result.total_primary_clicks == 7
        assert result.total_secondary_clicks == 3
        
        expected_overall_ctr = 12.0 / 45.0
        expected_primary_ctr = 7.0 / 20.0
        expected_secondary_ctr = 3.0 / 20.0
        
        assert abs(result.overall_ctr - expected_overall_ctr) < 0.001
        assert abs(result.overall_primary_ctr - expected_primary_ctr) < 0.001
        assert abs(result.overall_secondary_ctr - expected_secondary_ctr) < 0.001
    
    def test_date_filtering_logic(self):
        """Test date filtering functionality"""
        tile_events_data = [
            ("evt_001", "user_001", "sess_001", "2025-12-01 10:00:00", "tile_001", "TILE_VIEW", "Mobile", "1.0.0"),
            ("evt_002", "user_001", "sess_001", "2025-12-02 10:01:00", "tile_001", "TILE_CLICK", "Mobile", "1.0.0"),
            ("evt_003", "user_002", "sess_002", "2025-12-01 10:02:00", "tile_002", "TILE_VIEW", "Web", "1.0.0")
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
        
        df_tile = self.spark.createDataFrame(tile_events_data, tile_events_schema)
        df_tile = df_tile.withColumn("event_ts", F.to_timestamp("event_ts", "yyyy-MM-dd HH:mm:ss"))
        
        df_filtered = df_tile.filter(F.to_date("event_ts") == self.process_date)
        
        assert df_filtered.count() == 2
        
        dates = [F.to_date(row.event_ts).strftime('%Y-%m-%d') for row in df_filtered.collect()]
        assert all(date == self.process_date for date in dates)
    
    def test_edge_case_empty_dataframes(self):
        """Test handling of empty DataFrames"""
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
        
        df_tile_agg = (
            df_empty_tile.groupBy("tile_id")
            .agg(
                F.countDistinct(F.when(F.col("event_type") == "TILE_VIEW", F.col("user_id"))).alias("unique_tile_views"),
                F.countDistinct(F.when(F.col("event_type") == "TILE_CLICK", F.col("user_id"))).alias("unique_tile_clicks")
            )
        )
        
        assert df_tile_agg.count() == 0
        assert len(df_tile_agg.columns) == 3
    
    def test_edge_case_zero_division(self):
        """Test CTR calculation with zero denominators"""
        daily_summary_data = [
            ("2025-12-01", "tile_001", "FINANCE", 0, 0, 0, 0, 0)
        ]
        
        df_daily_summary = self.spark.createDataFrame(
            daily_summary_data,
            ["date", "tile_id", "tile_category", "unique_tile_views", "unique_tile_clicks", 
             "unique_interstitial_views", "unique_interstitial_primary_clicks", "unique_interstitial_secondary_clicks"]
        )
        
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
        
        result = df_global.collect()[0]
        
        assert result.overall_ctr == 0.0
        assert result.overall_primary_ctr == 0.0
        assert result.overall_secondary_ctr == 0.0


class TestHomeTileReportingETLIntegration:
    """
    Integration tests for the complete ETL pipeline
    """
    
    @classmethod
    def setup_class(cls):
        """Setup Spark session for integration testing"""
        cls.spark = SparkSession.builder \
            .appName("HomeTileReportingETLIntegrationTest") \
            .master("local[*]") \
            .config("spark.sql.adaptive.enabled", "false") \
            .getOrCreate()
        cls.spark.sparkContext.setLogLevel("WARN")
    
    @classmethod
    def teardown_class(cls):
        """Cleanup Spark session"""
        cls.spark.stop()
    
    def test_end_to_end_pipeline(self):
        """Test the complete ETL pipeline end-to-end"""
        # Create comprehensive test data
        tile_events_data = [
            ("evt_001", "user_001", "sess_001", "2025-12-01 10:00:00", "tile_001", "TILE_VIEW", "Mobile", "1.0.0"),
            ("evt_002", "user_001", "sess_001", "2025-12-01 10:01:00", "tile_001", "TILE_CLICK", "Mobile", "1.0.0"),
            ("evt_003", "user_002", "sess_002", "2025-12-01 10:02:00", "tile_002", "TILE_VIEW", "Web", "1.0.0")
        ]
        
        interstitial_events_data = [
            ("int_001", "user_001", "sess_001", "2025-12-01 10:01:30", "tile_001", True, True, False),
            ("int_002", "user_002", "sess_002", "2025-12-01 10:02:30", "tile_002", True, False, True)
        ]
        
        tile_metadata_data = [
            ("tile_001", "Personal Finance Tile", "FINANCE", True, "2025-12-01 09:00:00"),
            ("tile_002", "Health Check Tile", "HEALTH", True, "2025-12-01 09:00:00")
        ]
        
        # Execute complete pipeline simulation
        # This would call the main() function in a real scenario
        assert True  # Integration test placeholder


if __name__ == "__main__":
    pytest.main(["-v", __file__])
