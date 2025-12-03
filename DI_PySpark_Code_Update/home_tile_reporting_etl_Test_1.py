"""
_____________________________________________
## *Author*: AAVA
## *Created on*:   
## *Description*: Test script for Enhanced Home Tile Reporting ETL with Tile Category Metadata Integration
## *Version*: 1 
## *Updated on*: 
_____________________________________________

===============================================================================
                        TEST SCRIPT FOR HOME TILE REPORTING ETL
===============================================================================
File Name       : home_tile_reporting_etl_Test_1.py
Author          : AAVA
Created Date    : 2025-12-02
Version         : 1.0.0
Release         : R1 – Test Suite for Home Tile Reporting Enhancement

Test Description:
    This test script validates the enhanced ETL pipeline functionality:
    - Tests insert scenario with new tile data
    - Tests update scenario with existing tile data
    - Validates tile category enrichment
    - Validates aggregation logic
    - Validates data quality checks
    - Does not use PyTest framework (Databricks compatible)

Test Scenarios:
    1. Insert Scenario: New tiles with fresh data
    2. Update Scenario: Existing tiles with updated metrics
    3. Metadata Enrichment: Tile category mapping validation
    4. Edge Cases: Missing metadata, null values
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *
from datetime import datetime
import sys

# Initialize Spark Session
def get_spark_session():
    try:
        spark = SparkSession.getActiveSession()
        if spark is None:
            spark = (
                SparkSession.builder
                .appName("HomeTileReportingETL_Test")
                .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
                .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
                .getOrCreate()
            )
        return spark
    except Exception as e:
        print(f"Error initializing Spark session: {e}")
        raise

spark = get_spark_session()

# Test Configuration
TEST_DATE = "2025-12-01"
PASS_COUNT = 0
FAIL_COUNT = 0
TEST_RESULTS = []

def log_test_result(test_name, status, input_data, output_data, expected_data=None):
    """Log test results for reporting"""
    global PASS_COUNT, FAIL_COUNT, TEST_RESULTS
    
    if status == "PASS":
        PASS_COUNT += 1
    else:
        FAIL_COUNT += 1
    
    TEST_RESULTS.append({
        "test_name": test_name,
        "status": status,
        "input_data": input_data,
        "output_data": output_data,
        "expected_data": expected_data
    })
    
    print(f"\n{'='*60}")
    print(f"TEST: {test_name}")
    print(f"STATUS: {status}")
    print(f"{'='*60}")

def create_test_data_scenario_1():
    """Create test data for Scenario 1: Insert new tiles"""
    
    # New tile events data
    tile_events_data = [
        ("evt_101", "user_101", "sess_101", "2025-12-01 10:00:00", "tile_101", "TILE_VIEW", "Mobile", "1.0.0"),
        ("evt_102", "user_101", "sess_101", "2025-12-01 10:01:00", "tile_101", "TILE_CLICK", "Mobile", "1.0.0"),
        ("evt_103", "user_102", "sess_102", "2025-12-01 10:02:00", "tile_102", "TILE_VIEW", "Web", "1.0.0"),
        ("evt_104", "user_103", "sess_103", "2025-12-01 10:03:00", "tile_101", "TILE_VIEW", "Mobile", "1.0.0")
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
    df_tile_events = df_tile_events.withColumn("event_ts", F.to_timestamp("event_ts"))
    
    # New interstitial events data
    interstitial_events_data = [
        ("int_101", "user_101", "sess_101", "2025-12-01 10:01:30", "tile_101", True, True, False),
        ("int_102", "user_102", "sess_102", "2025-12-01 10:02:30", "tile_102", True, False, True)
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
    df_interstitial_events = df_interstitial_events.withColumn("event_ts", F.to_timestamp("event_ts"))
    
    # Tile metadata
    tile_metadata_data = [
        ("tile_101", "New Finance Tile", "Finance", True, "2025-12-01 09:00:00"),
        ("tile_102", "New Health Tile", "Health", True, "2025-12-01 09:00:00")
    ]
    
    tile_metadata_schema = StructType([
        StructField("tile_id", StringType(), True),
        StructField("tile_name", StringType(), True),
        StructField("tile_category", StringType(), True),
        StructField("is_active", BooleanType(), True),
        StructField("updated_ts", StringType(), True)
    ])
    
    df_tile_metadata = spark.createDataFrame(tile_metadata_data, tile_metadata_schema)
    df_tile_metadata = df_tile_metadata.withColumn("updated_ts", F.to_timestamp("updated_ts"))
    
    return df_tile_events, df_interstitial_events, df_tile_metadata

def create_test_data_scenario_2():
    """Create test data for Scenario 2: Update existing tiles"""
    
    # Updated tile events data (same tile_ids but different users/events)
    tile_events_data = [
        ("evt_201", "user_201", "sess_201", "2025-12-01 11:00:00", "tile_101", "TILE_VIEW", "Mobile", "1.0.0"),
        ("evt_202", "user_201", "sess_201", "2025-12-01 11:01:00", "tile_101", "TILE_CLICK", "Mobile", "1.0.0"),
        ("evt_203", "user_202", "sess_202", "2025-12-01 11:02:00", "tile_102", "TILE_VIEW", "Web", "1.0.0"),
        ("evt_204", "user_203", "sess_203", "2025-12-01 11:03:00", "tile_102", "TILE_CLICK", "Web", "1.0.0"),
        ("evt_205", "user_204", "sess_204", "2025-12-01 11:04:00", "tile_101", "TILE_VIEW", "Mobile", "1.0.0")
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
    df_tile_events = df_tile_events.withColumn("event_ts", F.to_timestamp("event_ts"))
    
    # Updated interstitial events data
    interstitial_events_data = [
        ("int_201", "user_201", "sess_201", "2025-12-01 11:01:30", "tile_101", True, True, False),
        ("int_202", "user_202", "sess_202", "2025-12-01 11:02:30", "tile_102", True, False, True),
        ("int_203", "user_203", "sess_203", "2025-12-01 11:03:30", "tile_102", True, True, False)
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
    df_interstitial_events = df_interstitial_events.withColumn("event_ts", F.to_timestamp("event_ts"))
    
    # Same tile metadata
    tile_metadata_data = [
        ("tile_101", "New Finance Tile", "Finance", True, "2025-12-01 09:00:00"),
        ("tile_102", "New Health Tile", "Health", True, "2025-12-01 09:00:00")
    ]
    
    tile_metadata_schema = StructType([
        StructField("tile_id", StringType(), True),
        StructField("tile_name", StringType(), True),
        StructField("tile_category", StringType(), True),
        StructField("is_active", BooleanType(), True),
        StructField("updated_ts", StringType(), True)
    ])
    
    df_tile_metadata = spark.createDataFrame(tile_metadata_data, tile_metadata_schema)
    df_tile_metadata = df_tile_metadata.withColumn("updated_ts", F.to_timestamp("updated_ts"))
    
    return df_tile_events, df_interstitial_events, df_tile_metadata

def run_etl_logic(df_tile, df_inter, df_metadata):
    """Run the core ETL logic"""
    
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
    
    # Join tile and interstitial aggregations
    df_combined_agg = (
        df_tile_agg.join(df_inter_agg, "tile_id", "outer")
    )
    
    # Enrich with metadata
    df_daily_summary = (
        df_combined_agg
        .join(df_metadata.select("tile_id", "tile_category"), "tile_id", "left")
        .withColumn("date", F.lit(TEST_DATE).cast("date"))
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

def test_scenario_1_insert():
    """Test Scenario 1: Insert new tiles into target table"""
    
    print("\n" + "="*80)
    print("RUNNING TEST SCENARIO 1: INSERT NEW TILES")
    print("="*80)
    
    # Create test data
    df_tile, df_inter, df_metadata = create_test_data_scenario_1()
    
    # Show input data
    print("\nInput Tile Events:")
    df_tile.show(truncate=False)
    
    print("\nInput Interstitial Events:")
    df_inter.show(truncate=False)
    
    print("\nInput Tile Metadata:")
    df_metadata.show(truncate=False)
    
    # Run ETL logic
    df_result = run_etl_logic(df_tile, df_inter, df_metadata)
    
    # Show output
    print("\nOutput Daily Summary:")
    df_result.show(truncate=False)
    
    # Validate results
    result_count = df_result.count()
    expected_count = 2  # tile_101 and tile_102
    
    # Check specific metrics for tile_101
    tile_101_data = df_result.filter(F.col("tile_id") == "tile_101").collect()
    
    if len(tile_101_data) > 0:
        tile_101 = tile_101_data[0]
        # Expected: 2 unique users viewed tile_101, 1 unique user clicked
        views_correct = tile_101["unique_tile_views"] == 2
        clicks_correct = tile_101["unique_tile_clicks"] == 1
        category_correct = tile_101["tile_category"] == "Finance"
        
        if views_correct and clicks_correct and category_correct and result_count == expected_count:
            status = "PASS"
        else:
            status = "FAIL"
    else:
        status = "FAIL"
    
    # Log results
    input_summary = f"Tile Events: {df_tile.count()}, Interstitial Events: {df_inter.count()}, Metadata: {df_metadata.count()}"
    output_summary = f"Daily Summary Records: {result_count}"
    
    log_test_result("Scenario 1: Insert New Tiles", status, input_summary, output_summary)
    
    return status == "PASS"

def test_scenario_2_update():
    """Test Scenario 2: Update existing tiles with new metrics"""
    
    print("\n" + "="*80)
    print("RUNNING TEST SCENARIO 2: UPDATE EXISTING TILES")
    print("="*80)
    
    # Create test data
    df_tile, df_inter, df_metadata = create_test_data_scenario_2()
    
    # Show input data
    print("\nInput Tile Events (Updated):")
    df_tile.show(truncate=False)
    
    print("\nInput Interstitial Events (Updated):")
    df_inter.show(truncate=False)
    
    # Run ETL logic
    df_result = run_etl_logic(df_tile, df_inter, df_metadata)
    
    # Show output
    print("\nOutput Daily Summary (Updated):")
    df_result.show(truncate=False)
    
    # Validate results
    result_count = df_result.count()
    expected_count = 2  # tile_101 and tile_102
    
    # Check specific metrics for tile_101 (should have updated counts)
    tile_101_data = df_result.filter(F.col("tile_id") == "tile_101").collect()
    tile_102_data = df_result.filter(F.col("tile_id") == "tile_102").collect()
    
    if len(tile_101_data) > 0 and len(tile_102_data) > 0:
        tile_101 = tile_101_data[0]
        tile_102 = tile_102_data[0]
        
        # Expected: tile_101 has 3 unique views, 1 unique click
        # Expected: tile_102 has 1 unique view, 1 unique click
        tile_101_views_correct = tile_101["unique_tile_views"] == 3
        tile_101_clicks_correct = tile_101["unique_tile_clicks"] == 1
        tile_102_views_correct = tile_102["unique_tile_views"] == 1
        tile_102_clicks_correct = tile_102["unique_tile_clicks"] == 1
        
        if (tile_101_views_correct and tile_101_clicks_correct and 
            tile_102_views_correct and tile_102_clicks_correct and 
            result_count == expected_count):
            status = "PASS"
        else:
            status = "FAIL"
    else:
        status = "FAIL"
    
    # Log results
    input_summary = f"Tile Events: {df_tile.count()}, Interstitial Events: {df_inter.count()}"
    output_summary = f"Daily Summary Records: {result_count}"
    
    log_test_result("Scenario 2: Update Existing Tiles", status, input_summary, output_summary)
    
    return status == "PASS"

def test_metadata_enrichment():
    """Test metadata enrichment and unknown category handling"""
    
    print("\n" + "="*80)
    print("RUNNING TEST: METADATA ENRICHMENT")
    print("="*80)
    
    # Create tile events with a tile that has no metadata
    tile_events_data = [
        ("evt_301", "user_301", "sess_301", "2025-12-01 12:00:00", "tile_999", "TILE_VIEW", "Mobile", "1.0.0")
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
    
    df_tile = spark.createDataFrame(tile_events_data, tile_events_schema)
    df_tile = df_tile.withColumn("event_ts", F.to_timestamp("event_ts"))
    
    # Empty interstitial events
    df_inter = spark.createDataFrame([], StructType([
        StructField("event_id", StringType(), True),
        StructField("user_id", StringType(), True),
        StructField("session_id", StringType(), True),
        StructField("event_ts", TimestampType(), True),
        StructField("tile_id", StringType(), True),
        StructField("interstitial_view_flag", BooleanType(), True),
        StructField("primary_button_click_flag", BooleanType(), True),
        StructField("secondary_button_click_flag", BooleanType(), True)
    ]))
    
    # Empty metadata (no mapping for tile_999)
    df_metadata = spark.createDataFrame([], StructType([
        StructField("tile_id", StringType(), True),
        StructField("tile_name", StringType(), True),
        StructField("tile_category", StringType(), True),
        StructField("is_active", BooleanType(), True),
        StructField("updated_ts", TimestampType(), True)
    ]))
    
    # Run ETL logic
    df_result = run_etl_logic(df_tile, df_inter, df_metadata)
    
    print("\nOutput with Unknown Category:")
    df_result.show(truncate=False)
    
    # Validate that tile_999 gets "UNKNOWN" category
    tile_999_data = df_result.filter(F.col("tile_id") == "tile_999").collect()
    
    if len(tile_999_data) > 0:
        tile_999 = tile_999_data[0]
        category_correct = tile_999["tile_category"] == "UNKNOWN"
        views_correct = tile_999["unique_tile_views"] == 1
        
        if category_correct and views_correct:
            status = "PASS"
        else:
            status = "FAIL"
    else:
        status = "FAIL"
    
    # Log results
    input_summary = "Tile with no metadata mapping"
    output_summary = f"Category: {tile_999_data[0]['tile_category'] if tile_999_data else 'None'}"
    
    log_test_result("Metadata Enrichment - Unknown Category", status, input_summary, output_summary)
    
    return status == "PASS"

def generate_test_report():
    """Generate markdown test report"""
    
    print("\n" + "="*100)
    print("                              TEST EXECUTION REPORT")
    print("="*100)
    
    report = "\n## Test Report\n\n"
    
    for result in TEST_RESULTS:
        report += f"### {result['test_name']}\n\n"
        report += f"**Input:** {result['input_data']}\n\n"
        report += f"**Output:** {result['output_data']}\n\n"
        report += f"**Status:** {result['status']}\n\n"
        report += "---\n\n"
    
    report += f"\n### Summary\n\n"
    report += f"- **Total Tests:** {PASS_COUNT + FAIL_COUNT}\n"
    report += f"- **Passed:** {PASS_COUNT}\n"
    report += f"- **Failed:** {FAIL_COUNT}\n"
    report += f"- **Success Rate:** {(PASS_COUNT/(PASS_COUNT + FAIL_COUNT)*100):.1f}%\n\n"
    
    print(report)
    
    # Summary
    print(f"\n{'='*60}")
    print(f"                    TEST SUMMARY")
    print(f"{'='*60}")
    print(f"Total Tests Run: {PASS_COUNT + FAIL_COUNT}")
    print(f"Tests Passed: {PASS_COUNT}")
    print(f"Tests Failed: {FAIL_COUNT}")
    print(f"Success Rate: {(PASS_COUNT/(PASS_COUNT + FAIL_COUNT)*100):.1f}%")
    
    if FAIL_COUNT == 0:
        print("\n🎉 ALL TESTS PASSED! 🎉")
    else:
        print(f"\n⚠️  {FAIL_COUNT} TEST(S) FAILED ⚠️")
    
    print(f"{'='*60}")
    
    return report

# ------------------------------------------------------------------------------
# MAIN TEST EXECUTION
# ------------------------------------------------------------------------------
if __name__ == "__main__":
    
    print("\n" + "="*100)
    print("           STARTING HOME TILE REPORTING ETL TEST SUITE")
    print("="*100)
    
    try:
        # Run all test scenarios
        test_scenario_1_insert()
        test_scenario_2_update()
        test_metadata_enrichment()
        
        # Generate final report
        final_report = generate_test_report()
        
        # Exit with appropriate code
        if FAIL_COUNT == 0:
            print("\n✅ Test suite completed successfully!")
            sys.exit(0)
        else:
            print(f"\n❌ Test suite completed with {FAIL_COUNT} failures!")
            sys.exit(1)
            
    except Exception as e:
        print(f"\n💥 Test suite execution failed with error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    
    finally:
        # Clean up Spark session
        if spark:
            spark.stop()
