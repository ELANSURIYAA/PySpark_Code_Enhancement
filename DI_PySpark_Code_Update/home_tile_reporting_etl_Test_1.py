"""
_____________________________________________
## *Author*: AAVA
## *Created on*:   
## *Description*: Test script for Enhanced Home Tile Reporting ETL with Tile Category Metadata Integration
## *Version*: 1 
## *Updated on*: 
_____________________________________________
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *
from datetime import datetime
import logging

# Initialize Spark Session
def get_spark_session():
    try:
        spark = SparkSession.getActiveSession()
        if spark is None:
            spark = SparkSession.builder.appName("HomeTileReportingETLTest").getOrCreate()
        return spark
    except Exception as e:
        print(f"Error initializing Spark session: {e}")
        raise

spark = get_spark_session()
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class HomeTileETLTester:
    def __init__(self):
        self.spark = spark
        self.process_date = "2025-12-01"
        self.test_results = []
    
    def create_test_data_scenario_1(self):
        """Create test data for Scenario 1: Insert new records"""
        # New tile events data
        tile_events_data = [
            ("evt_001", "user_001", "sess_001", "2025-12-01 10:00:00", "tile_001", "TILE_VIEW", "Mobile", "1.0.0"),
            ("evt_002", "user_001", "sess_001", "2025-12-01 10:01:00", "tile_001", "TILE_CLICK", "Mobile", "1.0.0"),
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
        
        df_tile_events = self.spark.createDataFrame(tile_events_data, tile_events_schema)
        df_tile_events = df_tile_events.withColumn("event_ts", F.to_timestamp("event_ts", "yyyy-MM-dd HH:mm:ss"))
        
        # New interstitial events data
        interstitial_events_data = [
            ("int_001", "user_001", "sess_001", "2025-12-01 10:01:30", "tile_001", True, True, False),
            ("int_002", "user_002", "sess_002", "2025-12-01 10:02:30", "tile_002", True, False, True)
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
        
        # Tile metadata
        tile_metadata_data = [
            ("tile_001", "Personal Finance Tile", "FINANCE", True, "2025-12-01 09:00:00"),
            ("tile_002", "Health Check Tile", "HEALTH", True, "2025-12-01 09:00:00")
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
        
        return df_tile_events, df_interstitial_events, df_tile_metadata
    
    def create_test_data_scenario_2(self):
        """Create test data for Scenario 2: Update existing records"""
        # Updated tile events data (same tile_ids but different metrics)
        tile_events_data = [
            ("evt_004", "user_003", "sess_003", "2025-12-01 11:00:00", "tile_001", "TILE_VIEW", "Mobile", "1.0.0"),
            ("evt_005", "user_003", "sess_003", "2025-12-01 11:01:00", "tile_001", "TILE_CLICK", "Mobile", "1.0.0"),
            ("evt_006", "user_004", "sess_004", "2025-12-01 11:02:00", "tile_002", "TILE_VIEW", "Web", "1.0.0"),
            ("evt_007", "user_004", "sess_004", "2025-12-01 11:03:00", "tile_002", "TILE_CLICK", "Web", "1.0.0")
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
        
        # Updated interstitial events data
        interstitial_events_data = [
            ("int_003", "user_003", "sess_003", "2025-12-01 11:01:30", "tile_001", True, True, False),
            ("int_004", "user_004", "sess_004", "2025-12-01 11:03:30", "tile_002", True, True, True)
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
        
        # Same tile metadata
        tile_metadata_data = [
            ("tile_001", "Personal Finance Tile", "FINANCE", True, "2025-12-01 09:00:00"),
            ("tile_002", "Health Check Tile", "HEALTH", True, "2025-12-01 09:00:00")
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
        
        return df_tile_events, df_interstitial_events, df_tile_metadata
    
    def run_etl_logic(self, df_tile, df_inter, df_metadata):
        """Run the ETL logic with provided data"""
        # Filter data for process date
        df_tile = df_tile.filter(F.to_date("event_ts") == self.process_date)
        df_inter = df_inter.filter(F.to_date("event_ts") == self.process_date)
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
            .withColumn("date", F.lit(self.process_date).cast("date"))
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
        
        return df_daily_summary
    
    def test_scenario_1_insert(self):
        """Test Scenario 1: Insert new records"""
        print("\n" + "="*80)
        print("TESTING SCENARIO 1: INSERT NEW RECORDS")
        print("="*80)
        
        try:
            # Create test data
            df_tile, df_inter, df_metadata = self.create_test_data_scenario_1()
            
            # Show input data
            print("\nInput Tile Events:")
            df_tile.select("tile_id", "user_id", "event_type").show()
            
            print("Input Interstitial Events:")
            df_inter.select("tile_id", "user_id", "interstitial_view_flag", "primary_button_click_flag", "secondary_button_click_flag").show()
            
            print("Input Tile Metadata:")
            df_metadata.select("tile_id", "tile_category", "tile_name").show()
            
            # Run ETL
            result_df = self.run_etl_logic(df_tile, df_inter, df_metadata)
            
            # Show output
            print("\nOutput Daily Summary:")
            result_df.show(truncate=False)
            
            # Validate results
            result_count = result_df.count()
            expected_tiles = ["tile_001", "tile_002"]
            actual_tiles = [row.tile_id for row in result_df.collect()]
            
            # Check if all expected tiles are present
            tiles_match = set(expected_tiles).issubset(set(actual_tiles))
            
            # Check if tile categories are correctly assigned
            categories_correct = True
            for row in result_df.collect():
                if row.tile_id == "tile_001" and row.tile_category != "FINANCE":
                    categories_correct = False
                elif row.tile_id == "tile_002" and row.tile_category != "HEALTH":
                    categories_correct = False
            
            status = "PASS" if (result_count > 0 and tiles_match and categories_correct) else "FAIL"
            
            self.test_results.append({
                "scenario": "Scenario 1: Insert",
                "status": status,
                "details": f"Records: {result_count}, Tiles Match: {tiles_match}, Categories Correct: {categories_correct}"
            })
            
            print(f"\nScenario 1 Status: {status}")
            return status == "PASS"
            
        except Exception as e:
            print(f"Scenario 1 failed with error: {e}")
            self.test_results.append({
                "scenario": "Scenario 1: Insert",
                "status": "FAIL",
                "details": f"Error: {str(e)}"
            })
            return False
    
    def test_scenario_2_update(self):
        """Test Scenario 2: Update existing records"""
        print("\n" + "="*80)
        print("TESTING SCENARIO 2: UPDATE EXISTING RECORDS")
        print("="*80)
        
        try:
            # Create test data
            df_tile, df_inter, df_metadata = self.create_test_data_scenario_2()
            
            # Show input data
            print("\nInput Tile Events (Updated):")
            df_tile.select("tile_id", "user_id", "event_type").show()
            
            print("Input Interstitial Events (Updated):")
            df_inter.select("tile_id", "user_id", "interstitial_view_flag", "primary_button_click_flag", "secondary_button_click_flag").show()
            
            # Run ETL
            result_df = self.run_etl_logic(df_tile, df_inter, df_metadata)
            
            # Show output
            print("\nOutput Daily Summary (Updated):")
            result_df.show(truncate=False)
            
            # Validate results
            result_count = result_df.count()
            expected_tiles = ["tile_001", "tile_002"]
            actual_tiles = [row.tile_id for row in result_df.collect()]
            
            # Check if all expected tiles are present
            tiles_match = set(expected_tiles).issubset(set(actual_tiles))
            
            # Check if metrics are updated (both tiles should have clicks now)
            metrics_updated = True
            for row in result_df.collect():
                if row.tile_id in ["tile_001", "tile_002"] and row.unique_tile_clicks == 0:
                    metrics_updated = False
            
            status = "PASS" if (result_count > 0 and tiles_match and metrics_updated) else "FAIL"
            
            self.test_results.append({
                "scenario": "Scenario 2: Update",
                "status": status,
                "details": f"Records: {result_count}, Tiles Match: {tiles_match}, Metrics Updated: {metrics_updated}"
            })
            
            print(f"\nScenario 2 Status: {status}")
            return status == "PASS"
            
        except Exception as e:
            print(f"Scenario 2 failed with error: {e}")
            self.test_results.append({
                "scenario": "Scenario 2: Update",
                "status": "FAIL",
                "details": f"Error: {str(e)}"
            })
            return False
    
    def generate_test_report(self):
        """Generate markdown test report"""
        print("\n" + "="*80)
        print("TEST EXECUTION REPORT")
        print("="*80)
        
        report = "\n## Test Report\n\n"
        
        for result in self.test_results:
            report += f"### {result['scenario']}\n"
            report += f"**Status:** {result['status']}\n"
            report += f"**Details:** {result['details']}\n\n"
        
        print(report)
        return report
    
    def run_all_tests(self):
        """Run all test scenarios"""
        print("Starting Home Tile ETL Test Suite...")
        
        # Run tests
        test1_passed = self.test_scenario_1_insert()
        test2_passed = self.test_scenario_2_update()
        
        # Generate report
        self.generate_test_report()
        
        # Overall result
        overall_status = "PASS" if (test1_passed and test2_passed) else "FAIL"
        print(f"\nOverall Test Suite Status: {overall_status}")
        
        return overall_status

# Main execution
if __name__ == "__main__":
    tester = HomeTileETLTester()
    tester.run_all_tests()
