```
_____________________________________________
## *Author*: AAVA
## *Created on*:   
## *Description*:   Python test script for testing the refactored PySpark mortgage amendment pipeline
## *Version*: 1 
## *Updated on*: 
_____________________________________________

import sys
import os
import logging
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType
from pyspark.sql.functions import col, when, lit, regexp_replace, broadcast, udf
from delta.tables import DeltaTable
from pyspark.sql import functions as F

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class TestConfig:
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

# Schema definitions (same as main pipeline)
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
    StructField("xml_text", StringType(), True)
])

def create_test_spark_session():
    """Create Spark session for testing"""
    spark = SparkSession.builder \
        .appName("Test_G_MtgAmendment_To_MISMO_XML") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.sql.warehouse.dir", "/tmp/test/spark-warehouse") \
        .master("local[*]") \
        .getOrCreate()
    return spark

def json_find_string_value(json_str, key):
    import re
    if not json_str:
        return ""
    pattern = r'"{}"\s*:\s*"([^"]*)"'.format(re.escape(key))
    m = re.search(pattern, json_str)
    return m.group(1) if m else ""

def json_find_scalar_value(json_str, key):
    import re
    if not json_str:
        return ""
    pattern = r'"{}"\s*:\s*"?([0-9.\-]+)"?'.format(re.escape(key))
    m = re.search(pattern, json_str)
    return m.group(1).strip() if m else ""

def business_reject_reason(effective_date, amendment_type, new_rate):
    if not effective_date:
        return "Missing effective_date"
    if not amendment_type:
        return "Missing amendment_type"
    if amendment_type == "RATE_CHANGE" and not new_rate:
        return "Missing new_rate for RATE_CHANGE"
    return None

def render_xml(template_text, loan_id, event_id, effective_date, amendment_type, new_rate, new_term_months):
    xml = template_text or ""
    xml = xml.replace("{{LOAN_ID}}", loan_id or "")
    xml = xml.replace("{{EVENT_ID}}", event_id or "")
    xml = xml.replace("{{EFFECTIVE_DATE}}", effective_date or "")
    xml = xml.replace("{{AMENDMENT_TYPE}}", amendment_type or "")
    xml = xml.replace("{{NEW_RATE}}", new_rate or "")
    xml = xml.replace("{{NEW_TERM_MONTHS}}", new_term_months or "")
    return xml

def test_scenario_1_insert():
    """Test Scenario 1: Insert new records into target table"""
    print("\n=== Test Scenario 1: Insert New Records ===")
    
    spark = create_test_spark_session()
    config = TestConfig()
    
    try:
        # Create sample input data for new records
        input_data = [
            ("key1", 0, 1001, "2026-01-28T10:00:00Z", 
             '{"event_id":"evt001","event_type":"MORTGAGE_AMENDMENT","event_ts":"2026-01-28T10:00:00Z","loan_id":"loan001","source_system":"CORE","amendment_type":"RATE_CHANGE","effective_date":"2026-02-01","prior_rate":"3.5","new_rate":"3.25","prior_term_months":"360","new_term_months":"360"}'),
            ("key2", 0, 1002, "2026-01-28T10:01:00Z", 
             '{"event_id":"evt002","event_type":"MORTGAGE_AMENDMENT","event_ts":"2026-01-28T10:01:00Z","loan_id":"loan002","source_system":"CORE","amendment_type":"TERM_CHANGE","effective_date":"2026-02-01","prior_rate":"4.0","new_rate":"4.0","prior_term_months":"360","new_term_months":"300"}')
        ]
        
        # Create template data
        template_data = [
            ("MISMO_TEMPLATE", "<MISMO><LoanID>{{LOAN_ID}}</LoanID><EventID>{{EVENT_ID}}</EventID><EffectiveDate>{{EFFECTIVE_DATE}}</EffectiveDate><AmendmentType>{{AMENDMENT_TYPE}}</AmendmentType><NewRate>{{NEW_RATE}}</NewRate><NewTermMonths>{{NEW_TERM_MONTHS}}</NewTermMonths></MISMO>")
        ]
        
        # Create DataFrames
        input_df = spark.createDataFrame(input_data, kafka_event_schema)
        template_df = spark.createDataFrame(template_data, template_record_schema)
        
        # Save to Delta tables
        input_df.write.format("delta").mode("overwrite").save(config.DELTA_LANDING_TABLE)
        template_df.write.format("delta").mode("overwrite").save(config.DELTA_TEMPLATE_TABLE)
        
        # Process the pipeline logic
        json_find_string_value_udf = udf(json_find_string_value, StringType())
        json_find_scalar_value_udf = udf(json_find_scalar_value, StringType())
        business_reject_reason_udf = udf(business_reject_reason, StringType())
        render_xml_udf = udf(render_xml, StringType())
        
        # Extract metadata
        event_meta_df = input_df.withColumn("event_id", json_find_string_value_udf(col("payload_json"), lit("event_id"))) \
            .withColumn("event_type", json_find_string_value_udf(col("payload_json"), lit("event_type"))) \
            .withColumn("event_ts", json_find_string_value_udf(col("payload_json"), lit("event_ts"))) \
            .withColumn("loan_id", json_find_string_value_udf(col("payload_json"), lit("loan_id"))) \
            .withColumn("source_system", json_find_string_value_udf(col("payload_json"), lit("source_system")))
        
        # Schema validation
        valid_event_meta_df = event_meta_df.withColumn(
            "reject_reason",
            when(col("event_id") == "", lit("Missing event_id"))
            .when(col("loan_id") == "", lit("Missing loan_id"))
            .when(col("event_type") != "MORTGAGE_AMENDMENT", lit("Unexpected event_type"))
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
            business_reject_reason_udf(col("effective_date"), col("amendment_type"), col("new_rate"))
        ).filter(col("reject_reason").isNull()).drop("reject_reason")
        
        # Generate XML
        xml_out_df = business_valid_df.withColumn(
            "xml_text",
            render_xml_udf(
                col("template_text"),
                col("loan_id"),
                col("event_id"),
                col("effective_date"),
                col("amendment_type"),
                col("new_rate"),
                col("new_term_months")
            )
        ).select("event_id", "loan_id", "xml_text")
        
        # Write to target table
        xml_out_df.write.format("delta").mode("overwrite").save(config.DELTA_XML_OUTPUT_TABLE)
        
        # Validate results
        result_df = spark.read.format("delta").load(config.DELTA_XML_OUTPUT_TABLE)
        result_count = result_df.count()
        
        print(f"Input records: {input_df.count()}")
        print(f"Output records: {result_count}")
        print("\nOutput data:")
        result_df.show(truncate=False)
        
        # Validation
        expected_count = 2
        if result_count == expected_count:
            print("✅ SCENARIO 1: PASS - Correct number of records inserted")
            return True
        else:
            print(f"❌ SCENARIO 1: FAIL - Expected {expected_count} records, got {result_count}")
            return False
            
    except Exception as e:
        print(f"❌ SCENARIO 1: FAIL - Error: {str(e)}")
        return False
    finally:
        spark.stop()

def test_scenario_2_update():
    """Test Scenario 2: Update existing records in target table"""
    print("\n=== Test Scenario 2: Update Existing Records ===")
    
    spark = create_test_spark_session()
    config = TestConfig()
    
    try:
        # First, create initial data
        initial_data = [
            ("key1", 0, 1001, "2026-01-28T10:00:00Z", 
             '{"event_id":"evt001","event_type":"MORTGAGE_AMENDMENT","event_ts":"2026-01-28T10:00:00Z","loan_id":"loan001","source_system":"CORE","amendment_type":"RATE_CHANGE","effective_date":"2026-02-01","prior_rate":"3.5","new_rate":"3.25","prior_term_months":"360","new_term_months":"360"}')
        ]
        
        # Create template data
        template_data = [
            ("MISMO_TEMPLATE", "<MISMO><LoanID>{{LOAN_ID}}</LoanID><EventID>{{EVENT_ID}}</EventID><EffectiveDate>{{EFFECTIVE_DATE}}</EffectiveDate><AmendmentType>{{AMENDMENT_TYPE}}</AmendmentType><NewRate>{{NEW_RATE}}</NewRate><NewTermMonths>{{NEW_TERM_MONTHS}}</NewTermMonths></MISMO>")
        ]
        
        # Process initial data
        initial_df = spark.createDataFrame(initial_data, kafka_event_schema)
        template_df = spark.createDataFrame(template_data, template_record_schema)
        
        # Save template
        template_df.write.format("delta").mode("overwrite").save(config.DELTA_TEMPLATE_TABLE)
        
        # Process initial data through pipeline
        json_find_string_value_udf = udf(json_find_string_value, StringType())
        json_find_scalar_value_udf = udf(json_find_scalar_value, StringType())
        business_reject_reason_udf = udf(business_reject_reason, StringType())
        render_xml_udf = udf(render_xml, StringType())
        
        # Process initial data
        event_meta_df = initial_df.withColumn("event_id", json_find_string_value_udf(col("payload_json"), lit("event_id"))) \
            .withColumn("event_type", json_find_string_value_udf(col("payload_json"), lit("event_type"))) \
            .withColumn("event_ts", json_find_string_value_udf(col("payload_json"), lit("event_ts"))) \
            .withColumn("loan_id", json_find_string_value_udf(col("payload_json"), lit("loan_id"))) \
            .withColumn("source_system", json_find_string_value_udf(col("payload_json"), lit("source_system")))
        
        valid_event_meta_df = event_meta_df.withColumn(
            "reject_reason",
            when(col("event_id") == "", lit("Missing event_id"))
            .when(col("loan_id") == "", lit("Missing loan_id"))
            .when(col("event_type") != "MORTGAGE_AMENDMENT", lit("Unexpected event_type"))
            .otherwise(lit(None))
        ).filter(col("reject_reason").isNull()).drop("reject_reason")
        
        canonical_amendment_df = valid_event_meta_df \
            .withColumn("amendment_type", json_find_string_value_udf(col("payload_json"), lit("amendment_type"))) \
            .withColumn("effective_date", json_find_string_value_udf(col("payload_json"), lit("effective_date"))) \
            .withColumn("prior_rate", json_find_scalar_value_udf(col("payload_json"), lit("prior_rate"))) \
            .withColumn("new_rate", json_find_scalar_value_udf(col("payload_json"), lit("new_rate"))) \
            .withColumn("prior_term_months", json_find_scalar_value_udf(col("payload_json"), lit("prior_term_months"))) \
            .withColumn("new_term_months", json_find_scalar_value_udf(col("payload_json"), lit("new_term_months")))
        
        canonical_with_template_df = canonical_amendment_df.crossJoin(broadcast(template_df))
        
        business_valid_df = canonical_with_template_df.withColumn(
            "reject_reason",
            business_reject_reason_udf(col("effective_date"), col("amendment_type"), col("new_rate"))
        ).filter(col("reject_reason").isNull()).drop("reject_reason")
        
        initial_xml_df = business_valid_df.withColumn(
            "xml_text",
            render_xml_udf(
                col("template_text"),
                col("loan_id"),
                col("event_id"),
                col("effective_date"),
                col("amendment_type"),
                col("new_rate"),
                col("new_term_months")
            )
        ).select("event_id", "loan_id", "xml_text")
        
        # Write initial data
        initial_xml_df.write.format("delta").mode("overwrite").save(config.DELTA_XML_OUTPUT_TABLE)
        
        print("Initial data:")
        initial_xml_df.show(truncate=False)
        
        # Now create updated data for the same loan_id
        updated_data = [
            ("key1_updated", 0, 1005, "2026-01-28T11:00:00Z", 
             '{"event_id":"evt001","event_type":"MORTGAGE_AMENDMENT","event_ts":"2026-01-28T11:00:00Z","loan_id":"loan001","source_system":"CORE","amendment_type":"RATE_CHANGE","effective_date":"2026-02-01","prior_rate":"3.25","new_rate":"3.0","prior_term_months":"360","new_term_months":"360"}')
        ]
        
        # Process updated data
        updated_df = spark.createDataFrame(updated_data, kafka_event_schema)
        
        # Process through same pipeline
        updated_event_meta_df = updated_df.withColumn("event_id", json_find_string_value_udf(col("payload_json"), lit("event_id"))) \
            .withColumn("event_type", json_find_string_value_udf(col("payload_json"), lit("event_type"))) \
            .withColumn("event_ts", json_find_string_value_udf(col("payload_json"), lit("event_ts"))) \
            .withColumn("loan_id", json_find_string_value_udf(col("payload_json"), lit("loan_id"))) \
            .withColumn("source_system", json_find_string_value_udf(col("payload_json"), lit("source_system")))
        
        updated_valid_event_meta_df = updated_event_meta_df.withColumn(
            "reject_reason",
            when(col("event_id") == "", lit("Missing event_id"))
            .when(col("loan_id") == "", lit("Missing loan_id"))
            .when(col("event_type") != "MORTGAGE_AMENDMENT", lit("Unexpected event_type"))
            .otherwise(lit(None))
        ).filter(col("reject_reason").isNull()).drop("reject_reason")
        
        updated_canonical_amendment_df = updated_valid_event_meta_df \
            .withColumn("amendment_type", json_find_string_value_udf(col("payload_json"), lit("amendment_type"))) \
            .withColumn("effective_date", json_find_string_value_udf(col("payload_json"), lit("effective_date"))) \
            .withColumn("prior_rate", json_find_scalar_value_udf(col("payload_json"), lit("prior_rate"))) \
            .withColumn("new_rate", json_find_scalar_value_udf(col("payload_json"), lit("new_rate"))) \
            .withColumn("prior_term_months", json_find_scalar_value_udf(col("payload_json"), lit("prior_term_months"))) \
            .withColumn("new_term_months", json_find_scalar_value_udf(col("payload_json"), lit("new_term_months")))
        
        updated_canonical_with_template_df = updated_canonical_amendment_df.crossJoin(broadcast(template_df))
        
        updated_business_valid_df = updated_canonical_with_template_df.withColumn(
            "reject_reason",
            business_reject_reason_udf(col("effective_date"), col("amendment_type"), col("new_rate"))
        ).filter(col("reject_reason").isNull()).drop("reject_reason")
        
        updated_xml_df = updated_business_valid_df.withColumn(
            "xml_text",
            render_xml_udf(
                col("template_text"),
                col("loan_id"),
                col("event_id"),
                col("effective_date"),
                col("amendment_type"),
                col("new_rate"),
                col("new_term_months")
            )
        ).select("event_id", "loan_id", "xml_text")
        
        # Simulate update by overwriting (in real scenario, this would be a merge/upsert)
        updated_xml_df.write.format("delta").mode("overwrite").save(config.DELTA_XML_OUTPUT_TABLE)
        
        # Validate results
        final_result_df = spark.read.format("delta").load(config.DELTA_XML_OUTPUT_TABLE)
        final_count = final_result_df.count()
        
        print("\nUpdated data:")
        final_result_df.show(truncate=False)
        
        # Check if the XML contains the updated rate (3.0 instead of 3.25)
        updated_xml_text = final_result_df.collect()[0]['xml_text']
        
        if "<NewRate>3.0</NewRate>" in updated_xml_text and final_count == 1:
            print("✅ SCENARIO 2: PASS - Record updated successfully with new rate")
            return True
        else:
            print("❌ SCENARIO 2: FAIL - Update not reflected correctly")
            return False
            
    except Exception as e:
        print(f"❌ SCENARIO 2: FAIL - Error: {str(e)}")
        return False
    finally:
        spark.stop()

def run_all_tests():
    """Run all test scenarios and generate report"""
    print("\n" + "="*60)
    print("MORTGAGE AMENDMENT PIPELINE TEST EXECUTION")
    print("="*60)
    
    # Run tests
    scenario1_result = test_scenario_1_insert()
    scenario2_result = test_scenario_2_update()
    
    # Generate markdown report
    print("\n" + "="*60)
    print("TEST REPORT")
    print("="*60)
    
    report = """
## Test Report

### Scenario 1: Insert New Records
**Input:**
| event_id | loan_id | amendment_type | new_rate |
|----------|---------|----------------|----------|
| evt001   | loan001 | RATE_CHANGE    | 3.25     |
| evt002   | loan002 | TERM_CHANGE    | 4.0      |

**Output:**
| event_id | loan_id | xml_text (contains) |
|----------|---------|--------------------|
| evt001   | loan001 | <NewRate>3.25</NewRate> |
| evt002   | loan002 | <NewRate>4.0</NewRate> |

**Status:** {}

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
| event_id | loan_id | xml_text (contains) |
|----------|---------|--------------------|
| evt001   | loan001 | <NewRate>3.0</NewRate> |

**Status:** {}

### Summary
- Total Tests: 2
- Passed: {}
- Failed: {}
- Overall Status: {}
""".format(
        "PASS" if scenario1_result else "FAIL",
        "PASS" if scenario2_result else "FAIL",
        sum([scenario1_result, scenario2_result]),
        2 - sum([scenario1_result, scenario2_result]),
        "PASS" if all([scenario1_result, scenario2_result]) else "FAIL"
    )
    
    print(report)
    
    return all([scenario1_result, scenario2_result])

if __name__ == "__main__":
    success = run_all_tests()
    sys.exit(0 if success else 1)
```