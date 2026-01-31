'''
_____________________________________________
## *Author*: AAVA
## *Created on*:   
## *Description*: Python test script for testing the refactored PySpark Mortgage Amendment to MISMO XML pipeline
## *Version*: 1 
## *Updated on*: 
_____________________________________________
'''

import os
import tempfile
import shutil
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType
from pyspark.sql.functions import col

# Import classes from the main pipeline
from G_MtgAmendment_To_MISMO_XML_LLD_Pipeline_1 import (
    Config, SchemaDefinitions, DataTransformationUtils, MortgageAmendmentProcessor
)

class TestMortgageAmendmentProcessor:
    """Test class for Mortgage Amendment Processor"""
    
    def __init__(self):
        self.spark = SparkSession.builder \
            .appName("TestMortgageAmendmentProcessor") \
            .config("spark.sql.warehouse.dir", "/tmp/spark-warehouse") \
            .getOrCreate()
        
        self.temp_dir = tempfile.mkdtemp()
        self.logger = DataTransformationUtils.setup_logging()
        self.test_results = []
    
    def cleanup(self):
        """Clean up test resources"""
        if os.path.exists(self.temp_dir):
            shutil.rmtree(self.temp_dir)
        self.spark.stop()
    
    def create_sample_data(self, scenario="insert"):
        """Create sample data for testing"""
        schemas = SchemaDefinitions()
        
        if scenario == "insert":
            # Scenario 1: New records to insert
            sample_data = [
                ("key1", 0, 1001, "2024-01-15T10:00:00Z", 
                 '{"event_id":"evt_001","event_type":"MORTGAGE_AMENDMENT","event_ts":"2024-01-15T10:00:00Z","loan_id":"LOAN_001","source_system":"CORE_SYSTEM","amendment_type":"RATE_CHANGE","effective_date":"2024-02-01","prior_rate":"4.5","new_rate":"3.75","prior_term_months":"360","new_term_months":"360"}'),
                ("key2", 0, 1002, "2024-01-15T10:05:00Z", 
                 '{"event_id":"evt_002","event_type":"MORTGAGE_AMENDMENT","event_ts":"2024-01-15T10:05:00Z","loan_id":"LOAN_002","source_system":"CORE_SYSTEM","amendment_type":"TERM_CHANGE","effective_date":"2024-02-15","prior_rate":"5.0","new_rate":"5.0","prior_term_months":"360","new_term_months":"240"}')
            ]
        else:
            # Scenario 2: Records for update (same loan_ids as existing)
            sample_data = [
                ("key3", 0, 1003, "2024-01-15T11:00:00Z", 
                 '{"event_id":"evt_003","event_type":"MORTGAGE_AMENDMENT","event_ts":"2024-01-15T11:00:00Z","loan_id":"LOAN_001","source_system":"CORE_SYSTEM","amendment_type":"RATE_CHANGE","effective_date":"2024-03-01","prior_rate":"3.75","new_rate":"3.25","prior_term_months":"360","new_term_months":"360"}'),
                ("key4", 0, 1004, "2024-01-15T11:05:00Z", 
                 '{"event_id":"evt_004","event_type":"MORTGAGE_AMENDMENT","event_ts":"2024-01-15T11:05:00Z","loan_id":"LOAN_002","source_system":"CORE_SYSTEM","amendment_type":"RATE_CHANGE","effective_date":"2024-03-15","prior_rate":"5.0","new_rate":"4.5","prior_term_months":"240","new_term_months":"240"}')
            ]
        
        return self.spark.createDataFrame(sample_data, schemas.get_kafka_event_schema())
    
    def create_template_data(self):
        """Create sample template data"""
        schemas = SchemaDefinitions()
        template_data = [
            ("MISMO_TEMPLATE", 
             "<MORTGAGE_AMENDMENT><LOAN_ID>{{LOAN_ID}}</LOAN_ID><EVENT_ID>{{EVENT_ID}}</EVENT_ID><EFFECTIVE_DATE>{{EFFECTIVE_DATE}}</EFFECTIVE_DATE><AMENDMENT_TYPE>{{AMENDMENT_TYPE}}</AMENDMENT_TYPE><NEW_RATE>{{NEW_RATE}}</NEW_RATE><NEW_TERM_MONTHS>{{NEW_TERM_MONTHS}}</NEW_TERM_MONTHS></MORTGAGE_AMENDMENT>")
        ]
        return self.spark.createDataFrame(template_data, schemas.get_template_record_schema())
    
    def setup_test_files(self, landing_df, template_df, config):
        """Setup test files in temporary directory"""
        # Write landing file
        landing_df.coalesce(1).write \
            .mode("overwrite") \
            .option("delimiter", "|") \
            .option("header", "false") \
            .csv(os.path.dirname(config.landing_file))
        
        # Write template file
        template_df.coalesce(1).write \
            .mode("overwrite") \
            .option("delimiter", "|") \
            .option("header", "false") \
            .csv(os.path.dirname(config.template_record_file))
    
    def run_scenario_test(self, scenario_name, scenario_type):
        """Run a specific test scenario"""
        print(f"\n=== Running {scenario_name} ===")
        
        try:
            # Create test configuration
            window_ts = "2024011510"
            config = Config(
                window_ts=window_ts,
                project_dir=self.temp_dir,
                aws_bucket_url=self.temp_dir
            )
            
            # Create directories
            os.makedirs(os.path.dirname(config.landing_file), exist_ok=True)
            os.makedirs(os.path.dirname(config.template_record_file), exist_ok=True)
            os.makedirs(os.path.dirname(config.error_log_path), exist_ok=True)
            os.makedirs(os.path.dirname(config.product_miss_path), exist_ok=True)
            os.makedirs(os.path.dirname(config.mismo_out_path), exist_ok=True)
            
            # Create sample data
            landing_df = self.create_sample_data(scenario_type)
            template_df = self.create_template_data()
            
            # Setup test files (simulate file system)
            # For testing, we'll directly use DataFrames instead of file I/O
            
            # Initialize processor
            processor = MortgageAmendmentProcessor(config, self.spark, self.logger)
            
            # Execute processing steps with sample data
            print("Step 1: Processing landing data...")
            event_meta_df = processor.extract_event_metadata(landing_df)
            
            print("Step 2: Schema validation...")
            schema_reject_df, valid_event_meta_df = processor.validate_schema_and_split(event_meta_df)
            
            print("Step 3: Canonicalization...")
            canonical_amendment_df = processor.canonicalize_amendment_data(valid_event_meta_df)
            
            print("Step 4: Template join...")
            canonical_with_template_df = processor.join_with_template(canonical_amendment_df, template_df)
            
            print("Step 5: Business validation and XML rendering...")
            business_reject_df, xml_out_df = processor.business_validate_and_render_xml(canonical_with_template_df)
            
            # Collect results for validation
            input_count = landing_df.count()
            output_count = xml_out_df.count()
            reject_count = schema_reject_df.count() + business_reject_df.count()
            
            # Display input data
            print("\nInput Data:")
            input_display_df = landing_df.select("kafka_key", "payload_json")
            input_rows = input_display_df.collect()
            
            # Display output data
            print("\nOutput Data:")
            output_rows = xml_out_df.collect()
            
            # Determine test result
            expected_output_count = input_count - reject_count
            test_passed = (output_count == expected_output_count and output_count > 0)
            
            # Store test result
            test_result = {
                'scenario': scenario_name,
                'input_count': input_count,
                'output_count': output_count,
                'reject_count': reject_count,
                'input_rows': input_rows,
                'output_rows': output_rows,
                'status': 'PASS' if test_passed else 'FAIL'
            }
            
            self.test_results.append(test_result)
            
            print(f"\nTest Result: {test_result['status']}")
            print(f"Input Records: {input_count}")
            print(f"Output Records: {output_count}")
            print(f"Rejected Records: {reject_count}")
            
            return test_result
            
        except Exception as e:
            print(f"Test failed with error: {str(e)}")
            test_result = {
                'scenario': scenario_name,
                'status': 'FAIL',
                'error': str(e),
                'input_count': 0,
                'output_count': 0,
                'reject_count': 0,
                'input_rows': [],
                'output_rows': []
            }
            self.test_results.append(test_result)
            return test_result
    
    def generate_markdown_report(self):
        """Generate markdown test report"""
        report = "# Test Report\n\n"
        
        for result in self.test_results:
            report += f"## {result['scenario']}\n\n"
            
            if result['status'] == 'FAIL' and 'error' in result:
                report += f"**Status: {result['status']}**\n\n"
                report += f"**Error:** {result['error']}\n\n"
                continue
            
            # Input section
            report += "### Input:\n"
            if result['input_rows']:
                report += "| kafka_key | payload_json |\n"
                report += "|-----------|--------------|\n"
                for row in result['input_rows']:
                    # Truncate long JSON for readability
                    json_str = row['payload_json'][:100] + "..." if len(row['payload_json']) > 100 else row['payload_json']
                    report += f"| {row['kafka_key']} | {json_str} |\n"
            else:
                report += "No input data\n"
            
            report += "\n"
            
            # Output section
            report += "### Output:\n"
            if result['output_rows']:
                report += "| event_id | loan_id | xml_text |\n"
                report += "|----------|---------|----------|\n"
                for row in result['output_rows']:
                    # Truncate long XML for readability
                    xml_str = row['xml_text'][:100] + "..." if len(row['xml_text']) > 100 else row['xml_text']
                    report += f"| {row['event_id']} | {row['loan_id']} | {xml_str} |\n"
            else:
                report += "No output data\n"
            
            report += "\n"
            
            # Statistics
            report += "### Statistics:\n"
            report += f"- Input Records: {result['input_count']}\n"
            report += f"- Output Records: {result['output_count']}\n"
            report += f"- Rejected Records: {result['reject_count']}\n\n"
            
            # Status
            report += f"**Status: {result['status']}**\n\n"
            report += "---\n\n"
        
        return report

def main():
    """Main test execution function"""
    print("Starting Mortgage Amendment Processor Tests")
    
    # Initialize test runner
    test_runner = TestMortgageAmendmentProcessor()
    
    try:
        # Run test scenarios
        test_runner.run_scenario_test("Scenario 1: Insert New Records", "insert")
        test_runner.run_scenario_test("Scenario 2: Update Existing Records", "update")
        
        # Generate and display report
        print("\n" + "="*50)
        print("MARKDOWN TEST REPORT")
        print("="*50)
        
        report = test_runner.generate_markdown_report()
        print(report)
        
        # Summary
        total_tests = len(test_runner.test_results)
        passed_tests = sum(1 for result in test_runner.test_results if result['status'] == 'PASS')
        
        print(f"\nTest Summary: {passed_tests}/{total_tests} tests passed")
        
    finally:
        # Cleanup
        test_runner.cleanup()

if __name__ == "__main__":
    main()
