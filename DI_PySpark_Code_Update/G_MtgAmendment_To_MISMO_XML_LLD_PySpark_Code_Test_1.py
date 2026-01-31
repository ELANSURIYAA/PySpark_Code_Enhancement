_____________________________________________
## *Author*: AAVA
## *Created on*:   
## *Description*: Python test script for Mortgage Amendment ETL Pipeline testing without PyTest framework
## *Version*: 1 
## *Updated on*: 
_____________________________________________

import sys
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType
from pyspark.sql.functions import col, when, lit, regexp_replace, broadcast, udf
import logging

# Import the main ETL classes (assuming they're in the same directory)
# For self-contained execution, we'll redefine the necessary classes

# =========================
# Test Configuration
# =========================
class TestConfig:
    """Test configuration for the ETL pipeline"""
    def __init__(self):
        self.window_ts = "TEST_2026012816"
        self.delta_raw_events = "/tmp/test_delta/raw_events"
        self.delta_rejects = "/tmp/test_delta/rejects"
        self.delta_mismo_output = "/tmp/test_delta/mismo_output"

# =========================
# Test Data Generator
# =========================
class TestDataGenerator:
    """Generate test data for different scenarios"""
    
    @staticmethod
    def get_kafka_event_schema():
        return StructType([
            StructField("kafka_key", StringType(), True),
            StructField("kafka_partition", IntegerType(), True),
            StructField("kafka_offset", LongType(), True),
            StructField("ingest_ts", StringType(), True),
            StructField("payload_json", StringType(), True)
        ])
    
    @staticmethod
    def generate_insert_scenario_data():
        """Generate data for insert scenario - new records"""
        return [
            ("key1", 0, 1001, "2026-01-29T10:00:00Z", 
             '{"event_id":"evt_001","event_type":"MORTGAGE_AMENDMENT","event_ts":"2026-01-29T10:00:00Z","loan_id":"loan_001","source_system":"CORE","amendment_type":"RATE_CHANGE","effective_date":"2026-02-01","prior_rate":"4.5","new_rate":"3.75","prior_term_months":"360","new_term_months":"360"}'),
            ("key2", 0, 1002, "2026-01-29T10:01:00Z", 
             '{"event_id":"evt_002","event_type":"MORTGAGE_AMENDMENT","event_ts":"2026-01-29T10:01:00Z","loan_id":"loan_002","source_system":"CORE","amendment_type":"TERM_CHANGE","effective_date":"2026-02-01","prior_rate":"5.0","new_rate":"5.0","prior_term_months":"360","new_term_months":"240"}')
        ]
    
    @staticmethod
    def generate_update_scenario_data():
        """Generate data for update scenario - existing keys with updates"""
        return [
            ("key1", 0, 1003, "2026-01-29T11:00:00Z", 
             '{"event_id":"evt_001","event_type":"MORTGAGE_AMENDMENT","event_ts":"2026-01-29T11:00:00Z","loan_id":"loan_001","source_system":"CORE","amendment_type":"RATE_CHANGE","effective_date":"2026-02-01","prior_rate":"3.75","new_rate":"3.25","prior_term_months":"360","new_term_months":"360"}'),
            ("key3", 0, 1004, "2026-01-29T11:01:00Z", 
             '{"event_id":"evt_003","event_type":"MORTGAGE_AMENDMENT","event_ts":"2026-01-29T11:01:00Z","loan_id":"loan_003","source_system":"CORE","amendment_type":"PAYMENT_CHANGE","effective_date":"2026-02-15","prior_rate":"4.0","new_rate":"4.0","prior_term_months":"300","new_term_months":"300"}')
        ]
    
    @staticmethod
    def generate_invalid_data():
        """Generate invalid data for reject testing"""
        return [
            ("key_invalid1", 0, 2001, "2026-01-29T12:00:00Z", 
             '{"event_id":"","event_type":"MORTGAGE_AMENDMENT","event_ts":"2026-01-29T12:00:00Z","loan_id":"loan_invalid1","source_system":"CORE","amendment_type":"RATE_CHANGE","effective_date":"2026-02-01","prior_rate":"4.5","new_rate":"3.75"}'),
            ("key_invalid2", 0, 2002, "2026-01-29T12:01:00Z", 
             '{"event_id":"evt_invalid2","event_type":"MORTGAGE_AMENDMENT","event_ts":"2026-01-29T12:01:00Z","loan_id":"loan_invalid2","source_system":"CORE","amendment_type":"RATE_CHANGE","effective_date":"","prior_rate":"4.5","new_rate":"3.75"}')
        ]

# =========================
# Simplified ETL for Testing
# =========================
class TestETLPipeline:
    """Simplified ETL pipeline for testing purposes"""
    
    def __init__(self, spark_session):
        self.spark = spark_session
        self.config = TestConfig()
        
        # Setup logging
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)
    
    def json_find_string_value(self, json_str, key):
        """Extract string value from JSON string"""
        import re
        if not json_str:
            return ""
        pattern = r'"{}"\s*:\s*"([^"]*)"'.format(re.escape(key))
        match = re.search(pattern, json_str)
        return match.group(1) if match else ""
    
    def json_find_scalar_value(self, json_str, key):
        """Extract scalar value from JSON string"""
        import re
        if not json_str:
            return ""
        pattern = r'"{}"\s*:\s*([0-9.\-]+)'.format(re.escape(key))
        match = re.search(pattern, json_str)
        return match.group(1).strip() if match else ""
    
    def render_xml(self, template_text, loan_id, event_id, effective_date, amendment_type, new_rate, new_term_months):
        """Render XML from template"""
        xml = template_text or ""
        replacements = {
            "{{LOAN_ID}}": loan_id or "",
            "{{EVENT_ID}}": event_id or "",
            "{{EFFECTIVE_DATE}}": effective_date or "",
            "{{AMENDMENT_TYPE}}": amendment_type or "",
            "{{NEW_RATE}}": new_rate or "",
            "{{NEW_TERM_MONTHS}}": new_term_months or ""
        }
        
        for token, value in replacements.items():
            xml = xml.replace(token, value)
        return xml
    
    def process_data(self, input_data):
        """Process input data through the ETL pipeline"""
        # Create DataFrame from input data
        input_df = self.spark.createDataFrame(input_data, TestDataGenerator.get_kafka_event_schema())
        
        # Register UDFs
        json_find_string_value_udf = udf(self.json_find_string_value, StringType())
        json_find_scalar_value_udf = udf(self.json_find_scalar_value, StringType())
        render_xml_udf = udf(self.render_xml, StringType())
        
        # Extract metadata
        event_meta_df = input_df.withColumn("event_id", json_find_string_value_udf(col("payload_json"), lit("event_id"))) \
            .withColumn("event_type", json_find_string_value_udf(col("payload_json"), lit("event_type"))) \
            .withColumn("event_ts", json_find_string_value_udf(col("payload_json"), lit("event_ts"))) \
            .withColumn("loan_id", json_find_string_value_udf(col("payload_json"), lit("loan_id"))) \
            .withColumn("source_system", json_find_string_value_udf(col("payload_json"), lit("source_system")))
        
        # Schema validation
        validated_df = event_meta_df.withColumn(
            "reject_reason",
            when(col("event_id") == "", lit("Missing event_id"))
            .when(col("loan_id") == "", lit("Missing loan_id"))
            .when(col("event_type") != "MORTGAGE_AMENDMENT", lit("Unexpected event_type"))
            .otherwise(lit(None))
        )
        
        # Split valid and invalid
        valid_df = validated_df.filter(col("reject_reason").isNull()).drop("reject_reason")
        reject_df = validated_df.filter(col("reject_reason").isNotNull())
        
        # Canonicalize valid records
        canonical_df = valid_df.withColumn("amendment_type", json_find_string_value_udf(col("payload_json"), lit("amendment_type"))) \
            .withColumn("effective_date", json_find_string_value_udf(col("payload_json"), lit("effective_date"))) \
            .withColumn("new_rate", json_find_scalar_value_udf(col("payload_json"), lit("new_rate"))) \
            .withColumn("new_term_months", json_find_scalar_value_udf(col("payload_json"), lit("new_term_months")))
        
        # Business validation
        def business_reject_reason(effective_date, amendment_type, new_rate):
            if not effective_date:
                return "Missing effective_date"
            if not amendment_type:
                return "Missing amendment_type"
            if amendment_type == "RATE_CHANGE" and not new_rate:
                return "Missing new_rate for RATE_CHANGE"
            return None
        
        business_reject_reason_udf = udf(business_reject_reason, StringType())
        
        business_validated_df = canonical_df.withColumn(
            "business_reject_reason",
            business_reject_reason_udf(col("effective_date"), col("amendment_type"), col("new_rate"))
        )
        
        business_valid_df = business_validated_df.filter(col("business_reject_reason").isNull()).drop("business_reject_reason")
        business_reject_df = business_validated_df.filter(col("business_reject_reason").isNotNull())
        
        # Create template
        template_text = "<MISMO><LoanID>{{LOAN_ID}}</LoanID><EventID>{{EVENT_ID}}</EventID><EffectiveDate>{{EFFECTIVE_DATE}}</EffectiveDate><AmendmentType>{{AMENDMENT_TYPE}}</AmendmentType><NewRate>{{NEW_RATE}}</NewRate><NewTermMonths>{{NEW_TERM_MONTHS}}</NewTermMonths></MISMO>"
        
        # Render XML
        xml_output_df = business_valid_df.withColumn(
            "xml_text",
            render_xml_udf(
                lit(template_text),
                col("loan_id"),
                col("event_id"),
                col("effective_date"),
                col("amendment_type"),
                col("new_rate"),
                col("new_term_months")
            )
        ).select("event_id", "loan_id", "xml_text")
        
        # Combine all rejects
        all_rejects_df = reject_df.select("event_id", "loan_id", "reject_reason").union(
            business_reject_df.select("event_id", "loan_id", "business_reject_reason").withColumnRenamed("business_reject_reason", "reject_reason")
        )
        
        return xml_output_df, all_rejects_df

# =========================
# Test Execution Framework
# =========================
class TestRunner:
    """Test runner for executing and validating test scenarios"""
    
    def __init__(self):
        self.spark = SparkSession.builder.appName("MortgageAmendmentETL_Test").getOrCreate()
        self.etl = TestETLPipeline(self.spark)
        self.test_results = []
    
    def run_insert_scenario_test(self):
        """Test Scenario 1: Insert new records"""
        print("\n" + "="*50)
        print("RUNNING TEST SCENARIO 1: INSERT")
        print("="*50)
        
        # Generate test data
        test_data = TestDataGenerator.generate_insert_scenario_data()
        
        # Process data
        xml_output, rejects = self.etl.process_data(test_data)
        
        # Collect results
        xml_results = xml_output.collect()
        reject_results = rejects.collect()
        
        # Validate results
        expected_records = 2  # Both records should be valid
        actual_records = len(xml_results)
        
        test_passed = actual_records == expected_records and len(reject_results) == 0
        
        # Store test result
        self.test_results.append({
            "scenario": "Insert",
            "input_count": len(test_data),
            "expected_output": expected_records,
            "actual_output": actual_records,
            "rejects": len(reject_results),
            "status": "PASS" if test_passed else "FAIL",
            "xml_results": xml_results
        })
        
        return test_passed, xml_results, reject_results
    
    def run_update_scenario_test(self):
        """Test Scenario 2: Update existing records"""
        print("\n" + "="*50)
        print("RUNNING TEST SCENARIO 2: UPDATE")
        print("="*50)
        
        # Generate test data
        test_data = TestDataGenerator.generate_update_scenario_data()
        
        # Process data
        xml_output, rejects = self.etl.process_data(test_data)
        
        # Collect results
        xml_results = xml_output.collect()
        reject_results = rejects.collect()
        
        # Validate results
        expected_records = 2  # Both records should be valid
        actual_records = len(xml_results)
        
        test_passed = actual_records == expected_records and len(reject_results) == 0
        
        # Store test result
        self.test_results.append({
            "scenario": "Update",
            "input_count": len(test_data),
            "expected_output": expected_records,
            "actual_output": actual_records,
            "rejects": len(reject_results),
            "status": "PASS" if test_passed else "FAIL",
            "xml_results": xml_results
        })
        
        return test_passed, xml_results, reject_results
    
    def run_invalid_data_test(self):
        """Test Scenario 3: Invalid data handling"""
        print("\n" + "="*50)
        print("RUNNING TEST SCENARIO 3: INVALID DATA")
        print("="*50)
        
        # Generate test data
        test_data = TestDataGenerator.generate_invalid_data()
        
        # Process data
        xml_output, rejects = self.etl.process_data(test_data)
        
        # Collect results
        xml_results = xml_output.collect()
        reject_results = rejects.collect()
        
        # Validate results - should have rejects, no valid output
        expected_rejects = 2  # Both records should be rejected
        actual_rejects = len(reject_results)
        
        test_passed = len(xml_results) == 0 and actual_rejects == expected_rejects
        
        # Store test result
        self.test_results.append({
            "scenario": "Invalid Data",
            "input_count": len(test_data),
            "expected_output": 0,
            "actual_output": len(xml_results),
            "rejects": actual_rejects,
            "status": "PASS" if test_passed else "FAIL",
            "xml_results": xml_results
        })
        
        return test_passed, xml_results, reject_results
    
    def generate_test_report(self):
        """Generate markdown test report"""
        print("\n" + "="*80)
        print("TEST EXECUTION REPORT")
        print("="*80)
        
        report = "\n## Test Report\n\n"
        
        for result in self.test_results:
            report += f"### Scenario: {result['scenario']}\n\n"
            
            # Input section
            report += "**Input:**\n"
            report += "| Field | Value |\n"
            report += "|-------|-------|\n"
            report += f"| Input Records | {result['input_count']} |\n"
            report += f"| Expected Output | {result['expected_output']} |\n\n"
            
            # Output section
            report += "**Output:**\n"
            if result['xml_results']:
                report += "| event_id | loan_id | xml_preview |\n"
                report += "|----------|---------|-------------|\n"
                for xml_row in result['xml_results']:
                    xml_preview = xml_row['xml_text'][:50] + "..." if len(xml_row['xml_text']) > 50 else xml_row['xml_text']
                    report += f"| {xml_row['event_id']} | {xml_row['loan_id']} | {xml_preview} |\n"
            else:
                report += "| Result | No valid output records |\n"
                report += "|--------|-------------------------|\n"
            
            report += "\n"
            
            # Status section
            report += f"**Status:** {result['status']}\n"
            report += f"**Actual Output Records:** {result['actual_output']}\n"
            report += f"**Reject Records:** {result['rejects']}\n\n"
            report += "---\n\n"
        
        # Summary
        total_tests = len(self.test_results)
        passed_tests = sum(1 for r in self.test_results if r['status'] == 'PASS')
        
        report += "## Summary\n\n"
        report += f"**Total Tests:** {total_tests}\n"
        report += f"**Passed:** {passed_tests}\n"
        report += f"**Failed:** {total_tests - passed_tests}\n"
        report += f"**Success Rate:** {(passed_tests/total_tests)*100:.1f}%\n\n"
        
        print(report)
        return report
    
    def run_all_tests(self):
        """Execute all test scenarios"""
        print("Starting Mortgage Amendment ETL Test Suite...")
        
        # Run all test scenarios
        test1_result = self.run_insert_scenario_test()
        test2_result = self.run_update_scenario_test()
        test3_result = self.run_invalid_data_test()
        
        # Generate and display report
        report = self.generate_test_report()
        
        # Return overall success
        all_passed = all(r['status'] == 'PASS' for r in self.test_results)
        
        print(f"\n{'='*80}")
        print(f"OVERALL TEST RESULT: {'PASS' if all_passed else 'FAIL'}")
        print(f"{'='*80}")
        
        return all_passed, report

# =========================
# Main Test Execution
# =========================
if __name__ == "__main__":
    print("Mortgage Amendment ETL Pipeline - Test Suite")
    print("=============================================")
    
    try:
        # Initialize test runner
        test_runner = TestRunner()
        
        # Execute all tests
        success, report = test_runner.run_all_tests()
        
        # Final status
        if success:
            print("\n✅ All tests passed successfully!")
        else:
            print("\n❌ Some tests failed. Please review the report above.")
            
    except Exception as e:
        print(f"\n❌ Test execution failed with error: {str(e)}")
        import traceback
        traceback.print_exc()
    
    finally:
        # Clean up Spark session
        try:
            SparkSession.getActiveSession().stop()
        except:
            pass
        
        print("\n=== TEST SUITE COMPLETED ===")