```
_____________________________________________
## *Author*: AAVA
## *Created on*:   
## *Description*:   Enhanced Python test script with comprehensive test scenarios and detailed reporting
## *Version*: 2 
## *Updated on*: 
## *Changes*: Added comprehensive test scenarios, enhanced error handling, performance metrics, and detailed markdown reporting
## *Reason*: Provide thorough testing coverage for the enhanced pipeline with better observability and validation
_____________________________________________

import sys
import os
import logging
import time
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType, TimestampType
from pyspark.sql.functions import col, when, lit, regexp_replace, broadcast, udf, current_timestamp
from delta.tables import DeltaTable
from pyspark.sql import functions as F

# Setup enhanced logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class EnhancedTestConfig:
    def __init__(self):
        self.DELTA_LANDING_TABLE = '/tmp/test/delta/landing/kafka_events'
        self.DELTA_RAW_EVENTS_TABLE = '/tmp/test/delta/raw/event_meta'
        self.DELTA_REJECTS_TABLE = '/tmp/test/delta/rejects/reject_records'
        self.DELTA_XML_OUTPUT_TABLE = '/tmp/test/delta/output/xml_records'
        self.DELTA_TEMPLATE_TABLE = '/tmp/test/delta/lookups/template_records'
        self.WINDOW_TS = '2026012816'
        self.AWS_BUCKET_URL = '/tmp/test/data/landing/mortgage_amendment'
        self.PROJECT_DIR = '/tmp/test/apps/mortgage-amendment-mismo'
        self.ERROR_LOG_PATH = f'/tmp/test/data/out/rejects/rejects_{self.WINDOW_TS}.dat'
        self.PRODUCT_MISS_PATH = f'/tmp/test/data/out/raw_events/raw_events_{self.WINDOW_TS}.dat'
        self.MISMO_OUT_PATH = f'/tmp/test/data/out/mismo_xml/mismo_amendment_{self.WINDOW_TS}.xml'
        self.TEMPLATE_RECORD_FILE = f'{self.PROJECT_DIR}/lookups/mismo_template_record.dat'
        self.LANDING_FILE = f'{self.AWS_BUCKET_URL}/amendment_events_{self.WINDOW_TS}.dat'
        
        # Enhanced configuration
        self.BROADCAST_THRESHOLD = 10485760  # 10MB
        self.CACHE_ENABLED = True
        self.MAX_REJECT_PERCENTAGE = 10.0
        self.MIN_EXPECTED_RECORDS = 1

# Enhanced schema definitions
kafka_event_schema = StructType([
    StructField("kafka_key", StringType(), True),
    StructField("kafka_partition", IntegerType(), True),
    StructField("kafka_offset", LongType(), True),
    StructField("ingest_ts", StringType(), True),
    StructField("payload_json", StringType(), True)
])

template_record_schema = StructType([
    StructField("template_key", StringType(), True),
    StructField("template_text", StringType(), True)
])

xml_out_record_schema = StructType([
    StructField("event_id", StringType(), True),
    StructField("loan_id", StringType(), True),
    StructField("xml_text", StringType(), True),
    StructField("processing_ts", TimestampType(), True),
    StructField("xml_length", IntegerType(), True)
])

class TestMetrics:
    def __init__(self):
        self.test_results = []
        self.start_time = time.time()
    
    def add_result(self, scenario, status, input_count, output_count, duration, details=None):
        self.test_results.append({
            'scenario': scenario,
            'status': status,
            'input_count': input_count,
            'output_count': output_count,
            'duration': duration,
            'details': details or {},
            'timestamp': datetime.now().isoformat()
        })
    
    def get_summary(self):
        total_tests = len(self.test_results)
        passed_tests = sum(1 for r in self.test_results if r['status'] == 'PASS')
        failed_tests = total_tests - passed_tests
        total_duration = time.time() - self.start_time
        
        return {
            'total_tests': total_tests,
            'passed': passed_tests,
            'failed': failed_tests,
            'success_rate': (passed_tests / total_tests * 100) if total_tests > 0 else 0,
            'total_duration': total_duration
        }

def create_enhanced_test_spark_session():
    """Create enhanced Spark session for testing"""
    spark = SparkSession.builder \
        .appName("Enhanced_Test_G_MtgAmendment_To_MISMO_XML") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.sql.warehouse.dir", "/tmp/test/spark-warehouse") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .master("local[*]") \
        .getOrCreate()
    return spark

# Enhanced UDF functions with better error handling
def json_find_string_value(json_str, key):
    import re
    try:
        if not json_str:
            return ""
        pattern = r'"{}"\s*:\s*"([^"]*)"'.format(re.escape(key))
        m = re.search(pattern, json_str)
        return m.group(1) if m else ""
    except Exception:
        return ""

def json_find_scalar_value(json_str, key):
    import re
    try:
        if not json_str:
            return ""
        pattern = r'"{}"\s*:\s*"?([0-9.\-]+)"?'.format(re.escape(key))
        m = re.search(pattern, json_str)
        return m.group(1).strip() if m else ""
    except Exception:
        return ""

def enhanced_business_reject_reason(effective_date, amendment_type, new_rate, new_term_months):
    try:
        if not effective_date or effective_date.strip() == "":
            return "Missing effective_date"
        if not amendment_type or amendment_type.strip() == "":
            return "Missing amendment_type"
        if amendment_type == "RATE_CHANGE" and (not new_rate or new_rate.strip() == ""):
            return "Missing new_rate for RATE_CHANGE"
        if amendment_type == "TERM_CHANGE" and (not new_term_months or new_term_months.strip() == ""):
            return "Missing new_term_months for TERM_CHANGE"
        # Additional validation for rate range
        if amendment_type == "RATE_CHANGE" and new_rate:
            try:
                rate_val = float(new_rate)
                if rate_val < 0 or rate_val > 50:
                    return "Invalid rate range (must be 0-50%)"
            except ValueError:
                return "Invalid rate format"
        return None
    except Exception:
        return "Business validation error"

def enhanced_render_xml(template_text, loan_id, event_id, effective_date, amendment_type, new_rate, new_term_months):
    try:
        xml = template_text or ""
        xml = xml.replace("{{LOAN_ID}}", loan_id or "")
        xml = xml.replace("{{EVENT_ID}}", event_id or "")
        xml = xml.replace("{{EFFECTIVE_DATE}}", effective_date or "")
        xml = xml.replace("{{AMENDMENT_TYPE}}", amendment_type or "")
        xml = xml.replace("{{NEW_RATE}}", new_rate or "")
        xml = xml.replace("{{NEW_TERM_MONTHS}}", new_term_months or "")
        return xml
    except Exception:
        return "<ERROR>XML_RENDERING_FAILED</ERROR>"

def test_scenario_1_insert_enhanced():
    """Enhanced Test Scenario 1: Insert new records with comprehensive validation"""
    print("\n=== Enhanced Test Scenario 1: Insert New Records ===")
    
    spark = create_enhanced_test_spark_session()
    config = EnhancedTestConfig()
    test_start = time.time()
    
    try:
        # Create comprehensive sample input data
        input_data = [
            ("key1", 0, 1001, "2026-01-28T10:00:00Z", 
             '{"event_id":"evt001","event_type":"MORTGAGE_AMENDMENT","event_ts":"2026-01-28T10:00:00Z","loan_id":"loan001","source_system":"CORE","amendment_type":"RATE_CHANGE","effective_date":"2026-02-01","prior_rate":"3.5","new_rate":"3.25","prior_term_months":"360","new_term_months":"360"}'),
            ("key2", 0, 1002, "2026-01-28T10:01:00Z", 
             '{"event_id":"evt002","event_type":"MORTGAGE_AMENDMENT","event_ts":"2026-01-28T10:01:00Z","loan_id":"loan002","source_system":"CORE","amendment_type":"TERM_CHANGE","effective_date":"2026-02-01","prior_rate":"4.0","new_rate":"4.0","prior_term_months":"360","new_term_months":"300"}'),
            ("key3", 0, 1003, "2026-01-28T10:02:00Z", 
             '{"event_id":"evt003","event_type":"MORTGAGE_AMENDMENT","event_ts":"2026-01-28T10:02:00Z","loan_id":"loan003","source_system":"CORE","amendment_type":"RATE_CHANGE","effective_date":"2026-02-01","prior_rate":"4.25","new_rate":"4.0","prior_term_months":"360","new_term_months":"360"}')
        ]
        
        # Enhanced template data with namespace
        template_data = [
            ("MISMO_TEMPLATE", 
             "<MISMO xmlns='http://www.mismo.org/residential/2009/schemas'><LoanID>{{LOAN_ID}}</LoanID><EventID>{{EVENT_ID}}</EventID><EffectiveDate>{{EFFECTIVE_DATE}}</EffectiveDate><AmendmentType>{{AMENDMENT_TYPE}}</AmendmentType><NewRate>{{NEW_RATE}}</NewRate><NewTermMonths>{{NEW_TERM_MONTHS}}</NewTermMonths><ProcessingTimestamp>" + datetime.now().isoformat() + "</ProcessingTimestamp></MISMO>")
        ]
        
        # Create DataFrames
        input_df = spark.createDataFrame(input_data, kafka_event_schema)
        template_df = spark.createDataFrame(template_data, template_record_schema)
        
        # Save to Delta tables
        input_df.write.format("delta").mode("overwrite").save(config.DELTA_LANDING_TABLE)
        template_df.write.format("delta").mode("overwrite").save(config.DELTA_TEMPLATE_TABLE)
        
        # Process through enhanced pipeline
        json_find_string_value_udf = udf(json_find_string_value, StringType())
        json_find_scalar_value_udf = udf(json_find_scalar_value, StringType())
        enhanced_business_reject_reason_udf = udf(enhanced_business_reject_reason, StringType())
        enhanced_render_xml_udf = udf(enhanced_render_xml, StringType())
        
        # Extract metadata with processing timestamp
        event_meta_df = input_df.withColumn("event_id", json_find_string_value_udf(col("payload_json"), lit("event_id"))) \
            .withColumn("event_type", json_find_string_value_udf(col("payload_json"), lit("event_type"))) \
            .withColumn("event_ts", json_find_string_value_udf(col("payload_json"), lit("event_ts"))) \
            .withColumn("loan_id", json_find_string_value_udf(col("payload_json"), lit("loan_id"))) \
            .withColumn("source_system", json_find_string_value_udf(col("payload_json"), lit("source_system"))) \
            .withColumn("processing_ts", current_timestamp())
        
        # Enhanced schema validation
        valid_event_meta_df = event_meta_df.withColumn(
            "reject_reason",
            when(col("event_id") == "", lit("Missing event_id"))
            .when(col("event_id").isNull(), lit("Null event_id"))
            .when(col("loan_id") == "", lit("Missing loan_id"))
            .when(col("loan_id").isNull(), lit("Null loan_id"))
            .when(col("event_type") != "MORTGAGE_AMENDMENT", lit("Unexpected event_type"))
            .when(col("payload_json").isNull(), lit("Null payload_json"))
            .otherwise(lit(None))
        ).filter(col("reject_reason").isNull()).drop("reject_reason")
        
        # Canonicalize with enhanced field extraction
        canonical_amendment_df = valid_event_meta_df \
            .withColumn("amendment_type", json_find_string_value_udf(col("payload_json"), lit("amendment_type"))) \
            .withColumn("effective_date", json_find_string_value_udf(col("payload_json"), lit("effective_date"))) \
            .withColumn("prior_rate", json_find_scalar_value_udf(col("payload_json"), lit("prior_rate"))) \
            .withColumn("new_rate", json_find_scalar_value_udf(col("payload_json"), lit("new_rate"))) \
            .withColumn("prior_term_months", json_find_scalar_value_udf(col("payload_json"), lit("prior_term_months"))) \
            .withColumn("new_term_months", json_find_scalar_value_udf(col("payload_json"), lit("new_term_months")))
        
        # Join with template (broadcast join)
        canonical_with_template_df = canonical_amendment_df.crossJoin(broadcast(template_df))
        
        # Enhanced business validation
        business_valid_df = canonical_with_template_df.withColumn(
            "reject_reason",
            enhanced_business_reject_reason_udf(col("effective_date"), col("amendment_type"), col("new_rate"), col("new_term_months"))
        ).filter(col("reject_reason").isNull()).drop("reject_reason")
        
        # Generate enhanced XML with metadata
        xml_out_df = business_valid_df.withColumn(
            "xml_text",
            enhanced_render_xml_udf(
                col("template_text"),
                col("loan_id"),
                col("event_id"),
                col("effective_date"),
                col("amendment_type"),
                col("new_rate"),
                col("new_term_months")
            )
        ).withColumn("xml_length", F.length(col("xml_text"))) \
         .select("event_id", "loan_id", "xml_text", "processing_ts", "xml_length")
        
        # Write to target table
        xml_out_df.write.format("delta").mode("overwrite").save(config.DELTA_XML_OUTPUT_TABLE)
        
        # Validate results
        result_df = spark.read.format("delta").load(config.DELTA_XML_OUTPUT_TABLE)
        result_count = result_df.count()
        input_count = input_df.count()
        
        print(f"Input records: {input_count}")
        print(f"Output records: {result_count}")
        print("\nOutput data:")
        result_df.show(truncate=False)
        
        # Enhanced validation
        expected_count = 3
        test_duration = time.time() - test_start
        
        # Validate XML content
        xml_validation_passed = True
        xml_records = result_df.collect()
        for record in xml_records:
            xml_text = record['xml_text']
            if not xml_text or "<ERROR>" in xml_text:
                xml_validation_passed = False
                break
            if not all(tag in xml_text for tag in ["<LoanID>", "<EventID>", "<EffectiveDate>"]):
                xml_validation_passed = False
                break
        
        if result_count == expected_count and xml_validation_passed:
            print("✅ ENHANCED SCENARIO 1: PASS - All records inserted with valid XML")
            return True, {
                'input_count': input_count,
                'output_count': result_count,
                'duration': test_duration,
                'xml_validation': 'PASS'
            }
        else:
            print(f"❌ ENHANCED SCENARIO 1: FAIL - Expected {expected_count} valid records, got {result_count}")
            return False, {
                'input_count': input_count,
                'output_count': result_count,
                'duration': test_duration,
                'xml_validation': 'FAIL' if not xml_validation_passed else 'PASS'
            }
            
    except Exception as e:
        test_duration = time.time() - test_start
        print(f"❌ ENHANCED SCENARIO 1: FAIL - Error: {str(e)}")
        return False, {
            'input_count': 0,
            'output_count': 0,
            'duration': test_duration,
            'error': str(e)
        }
    finally:
        spark.stop()

def test_scenario_2_update_enhanced():
    """Enhanced Test Scenario 2: Update existing records with comprehensive validation"""
    print("\n=== Enhanced Test Scenario 2: Update Existing Records ===")
    
    spark = create_enhanced_test_spark_session()
    config = EnhancedTestConfig()
    test_start = time.time()
    
    try:
        # Initial data setup
        initial_data = [
            ("key1", 0, 1001, "2026-01-28T10:00:00Z", 
             '{"event_id":"evt001","event_type":"MORTGAGE_AMENDMENT","event_ts":"2026-01-28T10:00:00Z","loan_id":"loan001","source_system":"CORE","amendment_type":"RATE_CHANGE","effective_date":"2026-02-01","prior_rate":"3.5","new_rate":"3.25","prior_term_months":"360","new_term_months":"360"}')
        ]
        
        # Enhanced template data
        template_data = [
            ("MISMO_TEMPLATE", 
             "<MISMO xmlns='http://www.mismo.org/residential/2009/schemas'><LoanID>{{LOAN_ID}}</LoanID><EventID>{{EVENT_ID}}</EventID><EffectiveDate>{{EFFECTIVE_DATE}}</EffectiveDate><AmendmentType>{{AMENDMENT_TYPE}}</AmendmentType><NewRate>{{NEW_RATE}}</NewRate><NewTermMonths>{{NEW_TERM_MONTHS}}</NewTermMonths><ProcessingTimestamp>" + datetime.now().isoformat() + "</ProcessingTimestamp></MISMO>")
        ]
        
        # Process initial data
        initial_df = spark.createDataFrame(initial_data, kafka_event_schema)
        template_df = spark.createDataFrame(template_data, template_record_schema)
        
        # Save template
        template_df.write.format("delta").mode("overwrite").save(config.DELTA_TEMPLATE_TABLE)
        
        # Process initial data through enhanced pipeline
        json_find_string_value_udf = udf(json_find_string_value, StringType())
        json_find_scalar_value_udf = udf(json_find_scalar_value, StringType())
        enhanced_business_reject_reason_udf = udf(enhanced_business_reject_reason, StringType())
        enhanced_render_xml_udf = udf(enhanced_render_xml, StringType())
        
        # Process initial data
        initial_processed_df = self._process_data_through_pipeline(
            spark, initial_df, template_df, config,
            json_find_string_value_udf, json_find_scalar_value_udf,
            enhanced_business_reject_reason_udf, enhanced_render_xml_udf
        )
        
        # Write initial data
        initial_processed_df.write.format("delta").mode("overwrite").save(config.DELTA_XML_OUTPUT_TABLE)
        
        print("Initial data:")
        initial_processed_df.show(truncate=False)
        
        # Create updated data with different rate
        updated_data = [
            ("key1_updated", 0, 1005, "2026-01-28T11:00:00Z", 
             '{"event_id":"evt001","event_type":"MORTGAGE_AMENDMENT","event_ts":"2026-01-28T11:00:00Z","loan_id":"loan001","source_system":"CORE","amendment_type":"RATE_CHANGE","effective_date":"2026-02-01","prior_rate":"3.25","new_rate":"3.0","prior_term_months":"360","new_term_months":"360"}')
        ]
        
        # Process updated data
        updated_df = spark.createDataFrame(updated_data, kafka_event_schema)
        
        updated_processed_df = self._process_data_through_pipeline(
            spark, updated_df, template_df, config,
            json_find_string_value_udf, json_find_scalar_value_udf,
            enhanced_business_reject_reason_udf, enhanced_render_xml_udf
        )
        
        # Simulate update by overwriting
        updated_processed_df.write.format("delta").mode("overwrite").save(config.DELTA_XML_OUTPUT_TABLE)
        
        # Validate results
        final_result_df = spark.read.format("delta").load(config.DELTA_XML_OUTPUT_TABLE)
        final_count = final_result_df.count()
        
        print("\nUpdated data:")
        final_result_df.show(truncate=False)
        
        # Enhanced validation
        test_duration = time.time() - test_start
        
        if final_count > 0:
            updated_xml_text = final_result_df.collect()[0]['xml_text']
            xml_length = final_result_df.collect()[0]['xml_length']
            
            # Validate the update
            update_successful = "<NewRate>3.0</NewRate>" in updated_xml_text and final_count == 1
            xml_valid = xml_length > 0 and "<ERROR>" not in updated_xml_text
            
            if update_successful and xml_valid:
                print("✅ ENHANCED SCENARIO 2: PASS - Record updated successfully with valid XML")
                return True, {
                    'input_count': 1,
                    'output_count': final_count,
                    'duration': test_duration,
                    'xml_length': xml_length,
                    'update_validation': 'PASS'
                }
            else:
                print("❌ ENHANCED SCENARIO 2: FAIL - Update validation failed")
                return False, {
                    'input_count': 1,
                    'output_count': final_count,
                    'duration': test_duration,
                    'xml_length': xml_length if final_count > 0 else 0,
                    'update_validation': 'FAIL'
                }
        else:
            print("❌ ENHANCED SCENARIO 2: FAIL - No output records")
            return False, {
                'input_count': 1,
                'output_count': 0,
                'duration': test_duration,
                'update_validation': 'FAIL'
            }
            
    except Exception as e:
        test_duration = time.time() - test_start
        print(f"❌ ENHANCED SCENARIO 2: FAIL - Error: {str(e)}")
        return False, {
            'input_count': 1,
            'output_count': 0,
            'duration': test_duration,
            'error': str(e)
        }
    finally:
        spark.stop()

def _process_data_through_pipeline(spark, input_df, template_df, config, 
                                 json_find_string_value_udf, json_find_scalar_value_udf,
                                 enhanced_business_reject_reason_udf, enhanced_render_xml_udf):
    """Helper method to process data through the pipeline"""
    
    # Extract metadata
    event_meta_df = input_df.withColumn("event_id", json_find_string_value_udf(col("payload_json"), lit("event_id"))) \
        .withColumn("event_type", json_find_string_value_udf(col("payload_json"), lit("event_type"))) \
        .withColumn("event_ts", json_find_string_value_udf(col("payload_json"), lit("event_ts"))) \
        .withColumn("loan_id", json_find_string_value_udf(col("payload_json"), lit("loan_id"))) \
        .withColumn("source_system", json_find_string_value_udf(col("payload_json"), lit("source_system"))) \
        .withColumn("processing_ts", current_timestamp())
    
    # Schema validation
    valid_event_meta_df = event_meta_df.withColumn(
        "reject_reason",
        when(col("event_id") == "", lit("Missing event_id"))
        .when(col("event_id").isNull(), lit("Null event_id"))
        .when(col("loan_id") == "", lit("Missing loan_id"))
        .when(col("loan_id").isNull(), lit("Null loan_id"))
        .when(col("event_type") != "MORTGAGE_AMENDMENT", lit("Unexpected event_type"))
        .when(col("payload_json").isNull(), lit("Null payload_json"))
        .otherwise(lit(None))
    ).filter(col("reject_reason").isNull()).drop("reject_reason")
    
    # Canonicalize
    canonical_amendment_df = valid_event_meta_df \
        .withColumn("amendment_type", json_find_string_value_udf(col("payload_json"), lit("amendment_type"))) \
        .withColumn("effective_date", json_find_string_value_udf(col("payload_json"), lit("effective_date"))) \
        .withColumn("prior_rate", json_find_scalar_value_udf(col("payload_json"), lit("prior_rate"))) \
        .withColumn("new_rate", json_find_scalar_value_udf(col("payload_json"), lit("new_rate"))) \
        .withColumn("prior_term_months", json_find_scalar_value_udf(col("payload_json"), lit("prior_term_months"))) \
        .withColumn("new_term_months", json_find_scalar_value_udf(col("payload_json"), lit("new_term_months")))
    
    # Join with template
    canonical_with_template_df = canonical_amendment_df.crossJoin(broadcast(template_df))
    
    # Business validation
    business_valid_df = canonical_with_template_df.withColumn(
        "reject_reason",
        enhanced_business_reject_reason_udf(col("effective_date"), col("amendment_type"), col("new_rate"), col("new_term_months"))
    ).filter(col("reject_reason").isNull()).drop("reject_reason")
    
    # Generate XML
    xml_out_df = business_valid_df.withColumn(
        "xml_text",
        enhanced_render_xml_udf(
            col("template_text"),
            col("loan_id"),
            col("event_id"),
            col("effective_date"),
            col("amendment_type"),
            col("new_rate"),
            col("new_term_months")
        )
    ).withColumn("xml_length", F.length(col("xml_text"))) \
     .select("event_id", "loan_id", "xml_text", "processing_ts", "xml_length")
    
    return xml_out_df

def test_scenario_3_error_handling():
    """Test Scenario 3: Error handling and edge cases"""
    print("\n=== Test Scenario 3: Error Handling and Edge Cases ===")
    
    spark = create_enhanced_test_spark_session()
    config = EnhancedTestConfig()
    test_start = time.time()
    
    try:
        # Create data with various error conditions
        error_data = [
            ("key1", 0, 1001, "2026-01-28T10:00:00Z", 
             '{"event_id":"","event_type":"MORTGAGE_AMENDMENT","event_ts":"2026-01-28T10:00:00Z","loan_id":"loan001","source_system":"CORE"}'),  # Missing event_id
            ("key2", 0, 1002, "2026-01-28T10:01:00Z", 
             '{"event_id":"evt002","event_type":"OTHER_EVENT","event_ts":"2026-01-28T10:01:00Z","loan_id":"loan002","source_system":"CORE"}'),  # Wrong event_type
            ("key3", 0, 1003, "2026-01-28T10:02:00Z", 
             '{"event_id":"evt003","event_type":"MORTGAGE_AMENDMENT","event_ts":"2026-01-28T10:02:00Z","loan_id":"loan003","source_system":"CORE","amendment_type":"RATE_CHANGE","effective_date":""}'),  # Missing effective_date
            ("key4", 0, 1004, "2026-01-28T10:03:00Z", None),  # Null payload
            ("key5", 0, 1005, "2026-01-28T10:04:00Z", 
             '{"event_id":"evt005","event_type":"MORTGAGE_AMENDMENT","event_ts":"2026-01-28T10:04:00Z","loan_id":"loan005","source_system":"CORE","amendment_type":"RATE_CHANGE","effective_date":"2026-02-01","new_rate":"75.5"}')  # Invalid rate range
        ]
        
        template_data = [
            ("MISMO_TEMPLATE", "<MISMO><LoanID>{{LOAN_ID}}</LoanID><EventID>{{EVENT_ID}}</EventID><EffectiveDate>{{EFFECTIVE_DATE}}</EffectiveDate><AmendmentType>{{AMENDMENT_TYPE}}</AmendmentType><NewRate>{{NEW_RATE}}</NewRate><NewTermMonths>{{NEW_TERM_MONTHS}}</NewTermMonths></MISMO>")
        ]
        
        # Create DataFrames
        input_df = spark.createDataFrame(error_data, kafka_event_schema)
        template_df = spark.createDataFrame(template_data, template_record_schema)
        
        # Save to Delta tables
        input_df.write.format("delta").mode("overwrite").save(config.DELTA_LANDING_TABLE)
        template_df.write.format("delta").mode("overwrite").save(config.DELTA_TEMPLATE_TABLE)
        
        # Process through pipeline (should handle errors gracefully)
        json_find_string_value_udf = udf(json_find_string_value, StringType())
        json_find_scalar_value_udf = udf(json_find_scalar_value, StringType())
        enhanced_business_reject_reason_udf = udf(enhanced_business_reject_reason, StringType())
        enhanced_render_xml_udf = udf(enhanced_render_xml, StringType())
        
        processed_df = _process_data_through_pipeline(
            spark, input_df, template_df, config,
            json_find_string_value_udf, json_find_scalar_value_udf,
            enhanced_business_reject_reason_udf, enhanced_render_xml_udf
        )
        
        # Write results
        processed_df.write.format("delta").mode("overwrite").save(config.DELTA_XML_OUTPUT_TABLE)
        
        # Validate error handling
        result_df = spark.read.format("delta").load(config.DELTA_XML_OUTPUT_TABLE)
        result_count = result_df.count()
        input_count = input_df.count()
        
        test_duration = time.time() - test_start
        
        print(f"Input records (with errors): {input_count}")
        print(f"Output records (valid only): {result_count}")
        
        # Should have 0 valid records due to various errors
        if result_count == 0:
            print("✅ SCENARIO 3: PASS - Error handling working correctly, no invalid records processed")
            return True, {
                'input_count': input_count,
                'output_count': result_count,
                'duration': test_duration,
                'error_handling': 'PASS'
            }
        else:
            print(f"❌ SCENARIO 3: FAIL - Expected 0 valid records, got {result_count}")
            return False, {
                'input_count': input_count,
                'output_count': result_count,
                'duration': test_duration,
                'error_handling': 'FAIL'
            }
            
    except Exception as e:
        test_duration = time.time() - test_start
        print(f"❌ SCENARIO 3: FAIL - Unexpected error: {str(e)}")
        return False, {
            'input_count': 0,
            'output_count': 0,
            'duration': test_duration,
            'error': str(e)
        }
    finally:
        spark.stop()

def run_all_enhanced_tests():
    """Run all enhanced test scenarios and generate comprehensive report"""
    print("\n" + "="*80)
    print("ENHANCED MORTGAGE AMENDMENT PIPELINE TEST EXECUTION")
    print("="*80)
    
    metrics = TestMetrics()
    
    # Run enhanced tests
    scenario1_result, scenario1_details = test_scenario_1_insert_enhanced()
    metrics.add_result("Insert New Records", "PASS" if scenario1_result else "FAIL", 
                      scenario1_details.get('input_count', 0), scenario1_details.get('output_count', 0),
                      scenario1_details.get('duration', 0), scenario1_details)
    
    scenario2_result, scenario2_details = test_scenario_2_update_enhanced()
    metrics.add_result("Update Existing Records", "PASS" if scenario2_result else "FAIL",
                      scenario2_details.get('input_count', 0), scenario2_details.get('output_count', 0),
                      scenario2_details.get('duration', 0), scenario2_details)
    
    scenario3_result, scenario3_details = test_scenario_3_error_handling()
    metrics.add_result("Error Handling", "PASS" if scenario3_result else "FAIL",
                      scenario3_details.get('input_count', 0), scenario3_details.get('output_count', 0),
                      scenario3_details.get('duration', 0), scenario3_details)
    
    # Generate comprehensive markdown report
    summary = metrics.get_summary()
    
    print("\n" + "="*80)
    print("ENHANCED TEST REPORT")
    print("="*80)
    
    report = f"""
## Enhanced Test Report

### Test Execution Summary
- **Total Tests**: {summary['total_tests']}
- **Passed**: {summary['passed']}
- **Failed**: {summary['failed']}
- **Success Rate**: {summary['success_rate']:.1f}%
- **Total Duration**: {summary['total_duration']:.2f} seconds
- **Execution Time**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

### Scenario 1: Insert New Records
**Input:**
| event_id | loan_id | amendment_type | new_rate | expected_output |
|----------|---------|----------------|----------|----------------|
| evt001   | loan001 | RATE_CHANGE    | 3.25     | Valid XML      |
| evt002   | loan002 | TERM_CHANGE    | 4.0      | Valid XML      |
| evt003   | loan003 | RATE_CHANGE    | 4.0      | Valid XML      |

**Output:**
| Records | XML Validation | Duration | Status |
|---------|---------------|----------|--------|
| {scenario1_details.get('output_count', 0)} | {scenario1_details.get('xml_validation', 'N/A')} | {scenario1_details.get('duration', 0):.2f}s | {'PASS' if scenario1_result else 'FAIL'} |

**Status:** {'✅ PASS' if scenario1_result else '❌ FAIL'}

### Scenario 2: Update Existing Records
**Input (Initial):**
| event_id | loan_id | amendment_type | new_rate |
|----------|---------|----------------|----------|
| evt001   | loan001 | RATE_CHANGE    | 3.25     |

**Input (Updated):**
| event_id | loan_id | amendment_type | new_rate |
|----------|---------|----------------|----------|
| evt001   | loan001 | RATE_CHANGE    | 3.0      |

**Output:**
| Records | XML Length | Update Validation | Duration | Status |
|---------|------------|------------------|----------|--------|
| {scenario2_details.get('output_count', 0)} | {scenario2_details.get('xml_length', 0)} | {scenario2_details.get('update_validation', 'N/A')} | {scenario2_details.get('duration', 0):.2f}s | {'PASS' if scenario2_result else 'FAIL'} |

**Status:** {'✅ PASS' if scenario2_result else '❌ FAIL'}

### Scenario 3: Error Handling and Edge Cases
**Input (Error Cases):**
| Case | Error Type | Expected Behavior |
|------|------------|------------------|
| 1 | Missing event_id | Reject |
| 2 | Wrong event_type | Reject |
| 3 | Missing effective_date | Reject |
| 4 | Null payload | Reject |
| 5 | Invalid rate range | Reject |

**Output:**
| Input Records | Output Records | Error Handling | Duration | Status |
|---------------|----------------|----------------|----------|--------|
| {scenario3_details.get('input_count', 0)} | {scenario3_details.get('output_count', 0)} | {scenario3_details.get('error_handling', 'N/A')} | {scenario3_details.get('duration', 0):.2f}s | {'PASS' if scenario3_result else 'FAIL'} |

**Status:** {'✅ PASS' if scenario3_result else '❌ FAIL'}

### Performance Metrics
| Metric | Value |
|--------|-------|
| Average Test Duration | {sum(r['duration'] for r in metrics.test_results) / len(metrics.test_results):.2f}s |
| Total Processing Time | {summary['total_duration']:.2f}s |
| Tests per Second | {summary['total_tests'] / summary['total_duration']:.2f} |

### Overall Summary
- **Pipeline Status**: {'✅ HEALTHY' if all([scenario1_result, scenario2_result, scenario3_result]) else '❌ ISSUES DETECTED'}
- **Data Quality**: {'✅ GOOD' if scenario1_result and scenario2_result else '❌ POOR'}
- **Error Handling**: {'✅ ROBUST' if scenario3_result else '❌ NEEDS IMPROVEMENT'}
- **Recommendation**: {'✅ READY FOR PRODUCTION' if all([scenario1_result, scenario2_result, scenario3_result]) else '❌ REQUIRES FIXES BEFORE DEPLOYMENT'}

### Detailed Test Results
```json
{metrics.test_results}
```
"""
    
    print(report)
    
    return all([scenario1_result, scenario2_result, scenario3_result]), summary

if __name__ == "__main__":
    success, summary = run_all_enhanced_tests()
    print(f"\n🎯 Final Result: {'SUCCESS' if success else 'FAILURE'}")
    print(f"📊 Success Rate: {summary['success_rate']:.1f}%")
    sys.exit(0 if success else 1)
```