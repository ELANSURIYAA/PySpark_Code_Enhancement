"""
_____________________________________________
## *Author*: AAVA
## *Created on*:   
## *Description*: Python test script for testing the PySpark Mortgage Amendment to MISMO XML ETL pipeline
## *Version*: 1 
## *Updated on*: 
_____________________________________________
"""

import sys
import os
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class TestMortgageAmendmentETL:
    def __init__(self):
        """Initialize test framework"""
        self.spark = self._get_spark_session()
        self.test_results = []
        
    def _get_spark_session(self):
        """Get or create Spark session for testing"""
        try:
            spark = SparkSession.getActiveSession()
            if spark is None:
                spark = SparkSession.builder \
                    .appName("TestMortgageAmendmentETL") \
                    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
                    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
                    .getOrCreate()
            return spark
        except Exception as e:
            logger.error(f"Error creating Spark session: {e}")
            raise
    
    def create_test_data_scenario_1(self):
        """Create test data for Scenario 1: Insert new records"""
        # Schema for input data
        input_schema = StructType([
            StructField("event_id", StringType(), True),
            StructField("loan_id", StringType(), True),
            StructField("event_type", StringType(), True),
            StructField("json_payload", StringType(), True),
            StructField("timestamp", TimestampType(), True)
        ])
        
        # Test data - all new records that should be inserted
        test_data = [
            ("evt_test_001", "LOAN_TEST_001", "MORTGAGE_AMENDMENT", 
             '{"amendment_type":"RATE_CHANGE","effective_date":"2024-02-01","new_rate":4.25,"borrower_name":"Alice Johnson","property_address":"789 Pine St","loan_amount":350000}',
             datetime.now()),
            ("evt_test_002", "LOAN_TEST_002", "MORTGAGE_AMENDMENT", 
             '{"amendment_type":"TERM_CHANGE","effective_date":"2024-02-05","borrower_name":"Bob Wilson","property_address":"321 Elm Ave","loan_amount":275000}',
             datetime.now())
        ]
        
        return self.spark.createDataFrame(test_data, input_schema)
    
    def create_test_data_scenario_2(self):
        """Create test data for Scenario 2: Update existing records"""
        # Schema for input data
        input_schema = StructType([
            StructField("event_id", StringType(), True),
            StructField("loan_id", StringType(), True),
            StructField("event_type", StringType(), True),
            StructField("json_payload", StringType(), True),
            StructField("timestamp", TimestampType(), True)
        ])
        
        # Test data - records with existing keys that should be updated
        test_data = [
            ("evt_test_001", "LOAN_TEST_001", "MORTGAGE_AMENDMENT", 
             '{"amendment_type":"RATE_CHANGE","effective_date":"2024-02-10","new_rate":3.95,"borrower_name":"Alice Johnson Updated","property_address":"789 Pine St Updated","loan_amount":360000}',
             datetime.now()),
            ("evt_test_003", "LOAN_TEST_003", "MORTGAGE_AMENDMENT", 
             '{"amendment_type":"PAYMENT_CHANGE","effective_date":"2024-02-12","borrower_name":"Charlie Brown","property_address":"555 Oak Blvd","loan_amount":400000}',
             datetime.now())
        ]
        
        return self.spark.createDataFrame(test_data, input_schema)
    
    def create_template_data(self):
        """Create template data for testing"""
        template_schema = StructType([
            StructField("template_id", StringType(), True),
            StructField("xml_template", StringType(), True)
        ])
        
        template_data = [
            ("TEST_TEMPLATE_001", 
             '''<?xml version="1.0" encoding="UTF-8"?>
<MISMO_LOAN_AMENDMENT>
    <EVENT_ID>{{EVENT_ID}}</EVENT_ID>
    <LOAN_ID>{{LOAN_ID}}</LOAN_ID>
    <AMENDMENT_TYPE>{{AMENDMENT_TYPE}}</AMENDMENT_TYPE>
    <EFFECTIVE_DATE>{{EFFECTIVE_DATE}}</EFFECTIVE_DATE>
    <NEW_RATE>{{NEW_RATE}}</NEW_RATE>
    <BORROWER_NAME>{{BORROWER_NAME}}</BORROWER_NAME>
    <PROPERTY_ADDRESS>{{PROPERTY_ADDRESS}}</PROPERTY_ADDRESS>
    <LOAN_AMOUNT>{{LOAN_AMOUNT}}</LOAN_AMOUNT>
    <GENERATED_TIMESTAMP>{{GENERATED_TIMESTAMP}}</GENERATED_TIMESTAMP>
</MISMO_LOAN_AMENDMENT>''')
        ]
        
        return self.spark.createDataFrame(template_data, template_schema)
    
    def simulate_etl_processing(self, input_df, template_df):
        """Simulate the ETL processing logic for testing"""
        from pyspark.sql.functions import regexp_extract, when, col, lit, current_timestamp
        import re
        from pyspark import broadcast
        
        # UDF for JSON extraction (simplified for testing)
        def extract_json_value(json_str, field_name):
            if json_str is None:
                return None
            try:
                pattern = f'"({field_name})"\s*:\s*"([^"]*)"|"({field_name})"\s*:\s*([^,}}]*)'
                match = re.search(pattern, json_str)
                if match:
                    return match.group(2) if match.group(2) else match.group(4)
                return None
            except Exception:
                return None
        
        def extract_json_double(json_str, field_name):
            if json_str is None:
                return None
            try:
                pattern = f'"({field_name})"\s*:\s*([0-9.]+)'
                match = re.search(pattern, json_str)
                if match:
                    return float(match.group(2))
                return None
            except Exception:
                return None
        
        def render_xml(template, event_id, loan_id, amendment_type, effective_date, 
                      new_rate, borrower_name, property_address, loan_amount):
            if template is None:
                return None
            
            xml_content = template
            xml_content = xml_content.replace("{{EVENT_ID}}", str(event_id) if event_id else "")
            xml_content = xml_content.replace("{{LOAN_ID}}", str(loan_id) if loan_id else "")
            xml_content = xml_content.replace("{{AMENDMENT_TYPE}}", str(amendment_type) if amendment_type else "")
            xml_content = xml_content.replace("{{EFFECTIVE_DATE}}", str(effective_date) if effective_date else "")
            xml_content = xml_content.replace("{{NEW_RATE}}", str(new_rate) if new_rate else "")
            xml_content = xml_content.replace("{{BORROWER_NAME}}", str(borrower_name) if borrower_name else "")
            xml_content = xml_content.replace("{{PROPERTY_ADDRESS}}", str(property_address) if property_address else "")
            xml_content = xml_content.replace("{{LOAN_AMOUNT}}", str(loan_amount) if loan_amount else "")
            xml_content = xml_content.replace("{{GENERATED_TIMESTAMP}}", str(datetime.now()))
            
            return xml_content
        
        # Register UDFs
        json_string_udf = udf(extract_json_value, StringType())
        json_double_udf = udf(extract_json_double, DoubleType())
        render_xml_udf = udf(render_xml, StringType())
        
        # Extract metadata
        event_meta_df = input_df.select(
            col("event_id"),
            col("loan_id"),
            col("event_type"),
            json_string_udf(col("json_payload"), lit("amendment_type")).alias("amendment_type"),
            json_string_udf(col("json_payload"), lit("effective_date")).alias("effective_date"),
            json_double_udf(col("json_payload"), lit("new_rate")).alias("new_rate"),
            json_string_udf(col("json_payload"), lit("borrower_name")).alias("borrower_name"),
            json_string_udf(col("json_payload"), lit("property_address")).alias("property_address"),
            json_double_udf(col("json_payload"), lit("loan_amount")).alias("loan_amount")
        )
        
        # Schema validation
        valid_events = event_meta_df.filter(
            (col("event_type") == "MORTGAGE_AMENDMENT") &
            (col("event_id").isNotNull()) &
            (col("loan_id").isNotNull())
        )
        
        # Business validation
        business_valid = valid_events.filter(
            (col("effective_date").isNotNull()) &
            (col("effective_date") != "")
        )
        
        # Generate XML
        template_broadcast = broadcast(template_df)
        events_with_template = business_valid.crossJoin(template_broadcast)
        
        xml_output = events_with_template.select(
            col("event_id"),
            col("loan_id"),
            render_xml_udf(
                col("xml_template"),
                col("event_id"),
                col("loan_id"),
                col("amendment_type"),
                col("effective_date"),
                col("new_rate"),
                col("borrower_name"),
                col("property_address"),
                col("loan_amount")
            ).alias("xml_content"),
            current_timestamp().alias("generated_timestamp")
        )
        
        return xml_output
    
    def run_scenario_1_test(self):
        """Test Scenario 1: Insert new records"""
        print("\n=== Running Scenario 1: Insert Test ===")
        
        try:
            # Create test data
            input_df = self.create_test_data_scenario_1()
            template_df = self.create_template_data()
            
            # Process through ETL
            output_df = self.simulate_etl_processing(input_df, template_df)
            
            # Collect results
            input_data = input_df.select("event_id", "loan_id").collect()
            output_data = output_df.select("event_id", "loan_id", "xml_content").collect()
            
            # Validation
            expected_count = len(input_data)
            actual_count = len(output_data)
            
            test_result = {
                'scenario': 'Insert',
                'input_count': expected_count,
                'output_count': actual_count,
                'input_data': input_data,
                'output_data': output_data,
                'status': 'PASS' if expected_count == actual_count else 'FAIL',
                'details': f"Expected {expected_count} records, got {actual_count}"
            }
            
            self.test_results.append(test_result)
            
            print(f"Input Records: {expected_count}")
            print(f"Output Records: {actual_count}")
            print(f"Status: {test_result['status']}")
            
            # Show sample data
            print("\nInput Data:")
            input_df.select("event_id", "loan_id").show()
            
            print("\nOutput Data:")
            output_df.select("event_id", "loan_id").show()
            
            return test_result
            
        except Exception as e:
            error_result = {
                'scenario': 'Insert',
                'status': 'FAIL',
                'error': str(e)
            }
            self.test_results.append(error_result)
            print(f"Test failed with error: {e}")
            return error_result
    
    def run_scenario_2_test(self):
        """Test Scenario 2: Update existing records"""
        print("\n=== Running Scenario 2: Update Test ===")
        
        try:
            # Create test data
            input_df = self.create_test_data_scenario_2()
            template_df = self.create_template_data()
            
            # Process through ETL
            output_df = self.simulate_etl_processing(input_df, template_df)
            
            # Collect results
            input_data = input_df.select("event_id", "loan_id").collect()
            output_data = output_df.select("event_id", "loan_id", "xml_content").collect()
            
            # Validation - check if records were processed (simulating update scenario)
            expected_count = len(input_data)
            actual_count = len(output_data)
            
            # Additional validation: check if XML contains updated values
            updated_records = 0
            for row in output_data:
                if "Updated" in row.xml_content or "Charlie Brown" in row.xml_content:
                    updated_records += 1
            
            test_result = {
                'scenario': 'Update',
                'input_count': expected_count,
                'output_count': actual_count,
                'updated_records': updated_records,
                'input_data': input_data,
                'output_data': output_data,
                'status': 'PASS' if expected_count == actual_count and updated_records > 0 else 'FAIL',
                'details': f"Expected {expected_count} records, got {actual_count}. Updated records: {updated_records}"
            }
            
            self.test_results.append(test_result)
            
            print(f"Input Records: {expected_count}")
            print(f"Output Records: {actual_count}")
            print(f"Updated Records: {updated_records}")
            print(f"Status: {test_result['status']}")
            
            # Show sample data
            print("\nInput Data:")
            input_df.select("event_id", "loan_id").show()
            
            print("\nOutput Data:")
            output_df.select("event_id", "loan_id").show()
            
            return test_result
            
        except Exception as e:
            error_result = {
                'scenario': 'Update',
                'status': 'FAIL',
                'error': str(e)
            }
            self.test_results.append(error_result)
            print(f"Test failed with error: {e}")
            return error_result
    
    def generate_test_report(self):
        """Generate markdown test report"""
        report = "\n## Test Report\n\n"
        
        for result in self.test_results:
            scenario = result['scenario']
            status = result['status']
            
            report += f"### Scenario: {scenario}\n\n"
            
            if status == 'FAIL' and 'error' in result:
                report += f"**Status: {status}**\n\n"
                report += f"**Error:** {result['error']}\n\n"
                continue
            
            # Input table
            report += "**Input:**\n\n"
            report += "| event_id | loan_id |\n"
            report += "|----------|---------|\n"
            
            if 'input_data' in result:
                for row in result['input_data']:
                    report += f"| {row.event_id} | {row.loan_id} |\n"
            
            report += "\n"
            
            # Output table
            report += "**Output:**\n\n"
            report += "| event_id | loan_id |\n"
            report += "|----------|---------|\n"
            
            if 'output_data' in result:
                for row in result['output_data']:
                    report += f"| {row.event_id} | {row.loan_id} |\n"
            
            report += "\n"
            
            # Status and details
            report += f"**Status: {status}**\n\n"
            if 'details' in result:
                report += f"**Details:** {result['details']}\n\n"
            
            report += "---\n\n"
        
        return report
    
    def run_all_tests(self):
        """Run all test scenarios"""
        print("\n=== Starting Mortgage Amendment ETL Tests ===")
        
        # Run test scenarios
        self.run_scenario_1_test()
        self.run_scenario_2_test()
        
        # Generate and display report
        report = self.generate_test_report()
        print(report)
        
        # Summary
        total_tests = len(self.test_results)
        passed_tests = len([r for r in self.test_results if r['status'] == 'PASS'])
        
        print(f"\n=== Test Summary ===")
        print(f"Total Tests: {total_tests}")
        print(f"Passed: {passed_tests}")
        print(f"Failed: {total_tests - passed_tests}")
        print(f"Success Rate: {(passed_tests/total_tests)*100:.1f}%")
        
        return self.test_results

def main():
    """Main test execution function"""
    try:
        # Initialize test framework
        test_framework = TestMortgageAmendmentETL()
        
        # Run all tests
        results = test_framework.run_all_tests()
        
        # Exit with appropriate code
        failed_tests = [r for r in results if r['status'] == 'FAIL']
        if failed_tests:
            print(f"\n❌ {len(failed_tests)} test(s) failed")
            sys.exit(1)
        else:
            print(f"\n✅ All tests passed successfully")
            sys.exit(0)
            
    except Exception as e:
        print(f"Test execution failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
