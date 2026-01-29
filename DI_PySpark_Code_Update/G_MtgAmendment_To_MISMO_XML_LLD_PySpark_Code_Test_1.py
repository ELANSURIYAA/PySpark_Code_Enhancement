_____________________________________________
## *Author*: AAVA
## *Created on*:   
## *Description*: Python test script for testing PySpark Mortgage Amendment to MISMO XML ETL job
## *Version*: 1 
## *Updated on*: 
_____________________________________________

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime
import sys
import os

# Add the pipeline module to path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

class MortgageAmendmentETLTester:
    def __init__(self):
        """Initialize the test framework"""
        self.spark = self._get_spark_session()
        self.test_results = []
        
    def _get_spark_session(self):
        """Get or create Spark session for testing"""
        try:
            spark = SparkSession.getActiveSession()
            if spark is None:
                spark = SparkSession.builder \
                    .appName("MortgageAmendmentETLTest") \
                    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
                    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
                    .getOrCreate()
            return spark
        except Exception as e:
            print(f"Error creating Spark session: {e}")
            raise
    
    def create_test_data_scenario_1(self):
        """Create test data for Scenario 1: Insert new records"""
        # New records that don't exist in target
        test_data = [
            ("evt_new_001", "loan_new_12345", "MORTGAGE_AMENDMENT", 
             '{"amendment_type":"RATE_CHANGE","effective_date":"2024-02-15","new_rate":"3.25","borrower_name":"Alice Johnson","property_address":"789 Pine St"}',
             datetime.now()),
            ("evt_new_002", "loan_new_67890", "MORTGAGE_AMENDMENT", 
             '{"amendment_type":"TERM_CHANGE","effective_date":"2024-02-20","new_term":"240","borrower_name":"Bob Wilson","property_address":"321 Elm Ave"}',
             datetime.now())
        ]
        
        template_data = [(
            "MISMO_TEMPLATE_001",
            '''<?xml version="1.0" encoding="UTF-8"?>
<MISMO_AMENDMENT>
    <EVENT_ID>{{EVENT_ID}}</EVENT_ID>
    <LOAN_ID>{{LOAN_ID}}</LOAN_ID>
    <AMENDMENT_TYPE>{{AMENDMENT_TYPE}}</AMENDMENT_TYPE>
    <EFFECTIVE_DATE>{{EFFECTIVE_DATE}}</EFFECTIVE_DATE>
    <NEW_RATE>{{NEW_RATE}}</NEW_RATE>
    <BORROWER_NAME>{{BORROWER_NAME}}</BORROWER_NAME>
    <PROPERTY_ADDRESS>{{PROPERTY_ADDRESS}}</PROPERTY_ADDRESS>
    <GENERATED_TIMESTAMP>{}</GENERATED_TIMESTAMP>
</MISMO_AMENDMENT>'''.format(datetime.now().isoformat())
        )]
        
        return test_data, template_data
    
    def create_test_data_scenario_2(self):
        """Create test data for Scenario 2: Update existing records"""
        # Records with existing keys but updated values
        test_data = [
            ("evt_update_001", "loan_existing_12345", "MORTGAGE_AMENDMENT", 
             '{"amendment_type":"RATE_CHANGE","effective_date":"2024-03-15","new_rate":"2.95","borrower_name":"Charlie Brown Updated","property_address":"456 Updated St"}',
             datetime.now()),
            ("evt_update_002", "loan_existing_67890", "MORTGAGE_AMENDMENT", 
             '{"amendment_type":"PAYMENT_CHANGE","effective_date":"2024-03-20","new_payment":"2500","borrower_name":"Diana Prince Updated","property_address":"789 Updated Ave"}',
             datetime.now())
        ]
        
        # Existing records in target (simulating current state)
        existing_data = [
            ("evt_existing_001", "loan_existing_12345", 
             '''<?xml version="1.0" encoding="UTF-8"?>
<MISMO_AMENDMENT>
    <EVENT_ID>evt_existing_001</EVENT_ID>
    <LOAN_ID>loan_existing_12345</LOAN_ID>
    <AMENDMENT_TYPE>RATE_CHANGE</AMENDMENT_TYPE>
    <EFFECTIVE_DATE>2024-01-15</EFFECTIVE_DATE>
    <NEW_RATE>3.75</NEW_RATE>
    <BORROWER_NAME>Charlie Brown</BORROWER_NAME>
    <PROPERTY_ADDRESS>456 Original St</PROPERTY_ADDRESS>
</MISMO_AMENDMENT>''',
             datetime(2024, 1, 15)),
            ("evt_existing_002", "loan_existing_67890", 
             '''<?xml version="1.0" encoding="UTF-8"?>
<MISMO_AMENDMENT>
    <EVENT_ID>evt_existing_002</EVENT_ID>
    <LOAN_ID>loan_existing_67890</LOAN_ID>
    <AMENDMENT_TYPE>TERM_CHANGE</AMENDMENT_TYPE>
    <EFFECTIVE_DATE>2024-01-20</EFFECTIVE_DATE>
    <NEW_TERM>360</NEW_TERM>
    <BORROWER_NAME>Diana Prince</BORROWER_NAME>
    <PROPERTY_ADDRESS>789 Original Ave</PROPERTY_ADDRESS>
</MISMO_AMENDMENT>''',
             datetime(2024, 1, 20))
        ]
        
        template_data = [(
            "MISMO_TEMPLATE_001",
            '''<?xml version="1.0" encoding="UTF-8"?>
<MISMO_AMENDMENT>
    <EVENT_ID>{{EVENT_ID}}</EVENT_ID>
    <LOAN_ID>{{LOAN_ID}}</LOAN_ID>
    <AMENDMENT_TYPE>{{AMENDMENT_TYPE}}</AMENDMENT_TYPE>
    <EFFECTIVE_DATE>{{EFFECTIVE_DATE}}</EFFECTIVE_DATE>
    <NEW_RATE>{{NEW_RATE}}</NEW_RATE>
    <BORROWER_NAME>{{BORROWER_NAME}}</BORROWER_NAME>
    <PROPERTY_ADDRESS>{{PROPERTY_ADDRESS}}</PROPERTY_ADDRESS>
    <GENERATED_TIMESTAMP>{}</GENERATED_TIMESTAMP>
</MISMO_AMENDMENT>'''.format(datetime.now().isoformat())
        )]
        
        return test_data, existing_data, template_data
    
    def run_etl_with_test_data(self, test_data, template_data):
        """Run ETL with specific test data"""
        from G_MtgAmendment_To_MISMO_XML_LLD_PySpark_Code_Pipeline_1 import MortgageAmendmentToMISMOXML
        
        # Create ETL instance
        etl_job = MortgageAmendmentToMISMOXML()
        
        # Override the create_sample_data method with test data
        etl_job.create_sample_data = lambda: (test_data, template_data)
        
        # Run ETL
        xml_output, rejects, raw_events = etl_job.run_etl()
        
        return xml_output, rejects, raw_events
    
    def validate_insert_scenario(self, xml_output, expected_count):
        """Validate insert scenario results"""
        actual_count = xml_output.count()
        
        # Check if all expected records are inserted
        success = actual_count == expected_count
        
        # Validate XML content contains expected tokens replaced
        xml_sample = xml_output.collect()[0] if actual_count > 0 else None
        xml_valid = False
        
        if xml_sample:
            xml_content = xml_sample['xml_content']
            # Check if tokens are replaced (no {{}} patterns should remain)
            xml_valid = '{{' not in xml_content and '}}' not in xml_content
        
        return success and xml_valid, actual_count, xml_sample
    
    def validate_update_scenario(self, xml_output, existing_data, expected_updates):
        """Validate update scenario results"""
        actual_count = xml_output.count()
        
        # In this ETL, updates are handled as new inserts (append mode)
        # Check if new records are created for updates
        success = actual_count == expected_updates
        
        # Validate that updated content is different from original
        xml_sample = xml_output.collect()[0] if actual_count > 0 else None
        update_valid = False
        
        if xml_sample:
            xml_content = xml_sample['xml_content']
            # Check if tokens are replaced and content is updated
            update_valid = ('Updated' in xml_content or '2.95' in xml_content) and '{{' not in xml_content
        
        return success and update_valid, actual_count, xml_sample
    
    def test_scenario_1_insert(self):
        """Test Scenario 1: Insert new records into target table"""
        print("\n" + "="*50)
        print("TESTING SCENARIO 1: INSERT NEW RECORDS")
        print("="*50)
        
        try:
            # Create test data
            test_data, template_data = self.create_test_data_scenario_1()
            
            print("\nInput Data:")
            input_df = self.spark.createDataFrame(test_data, StructType([
                StructField("event_id", StringType(), True),
                StructField("loan_id", StringType(), True),
                StructField("event_type", StringType(), True),
                StructField("json_payload", StringType(), True),
                StructField("timestamp", TimestampType(), True)
            ]))
            
            input_df.select("event_id", "loan_id", "event_type").show()
            
            # Run ETL
            xml_output, rejects, raw_events = self.run_etl_with_test_data(test_data, template_data)
            
            # Validate results
            is_valid, actual_count, xml_sample = self.validate_insert_scenario(xml_output, 2)
            
            print("\nOutput Data:")
            xml_output.select("event_id", "loan_id").show()
            
            print("\nSample XML Content:")
            if xml_sample:
                print(xml_sample['xml_content'][:200] + "...")
            
            # Test result
            status = "PASS" if is_valid else "FAIL"
            result = {
                'scenario': 'Insert New Records',
                'expected_count': 2,
                'actual_count': actual_count,
                'status': status,
                'details': f"Expected 2 new records, got {actual_count}"
            }
            
            self.test_results.append(result)
            print(f"\nScenario 1 Status: {status}")
            
            return result
            
        except Exception as e:
            error_result = {
                'scenario': 'Insert New Records',
                'expected_count': 2,
                'actual_count': 0,
                'status': 'FAIL',
                'details': f"Error: {str(e)}"
            }
            self.test_results.append(error_result)
            print(f"\nScenario 1 Status: FAIL - {str(e)}")
            return error_result
    
    def test_scenario_2_update(self):
        """Test Scenario 2: Update existing records in target table"""
        print("\n" + "="*50)
        print("TESTING SCENARIO 2: UPDATE EXISTING RECORDS")
        print("="*50)
        
        try:
            # Create test data
            test_data, existing_data, template_data = self.create_test_data_scenario_2()
            
            print("\nInput Data (Updates):")
            input_df = self.spark.createDataFrame(test_data, StructType([
                StructField("event_id", StringType(), True),
                StructField("loan_id", StringType(), True),
                StructField("event_type", StringType(), True),
                StructField("json_payload", StringType(), True),
                StructField("timestamp", TimestampType(), True)
            ]))
            
            input_df.select("event_id", "loan_id", "event_type").show()
            
            print("\nExisting Data (Before Update):")
            existing_df = self.spark.createDataFrame(existing_data, StructType([
                StructField("event_id", StringType(), True),
                StructField("loan_id", StringType(), True),
                StructField("xml_content", StringType(), True),
                StructField("generated_timestamp", TimestampType(), True)
            ]))
            
            existing_df.select("event_id", "loan_id").show()
            
            # Run ETL
            xml_output, rejects, raw_events = self.run_etl_with_test_data(test_data, template_data)
            
            # Validate results
            is_valid, actual_count, xml_sample = self.validate_update_scenario(xml_output, existing_data, 2)
            
            print("\nOutput Data (After Update):")
            xml_output.select("event_id", "loan_id").show()
            
            print("\nSample Updated XML Content:")
            if xml_sample:
                print(xml_sample['xml_content'][:200] + "...")
            
            # Test result
            status = "PASS" if is_valid else "FAIL"
            result = {
                'scenario': 'Update Existing Records',
                'expected_count': 2,
                'actual_count': actual_count,
                'status': status,
                'details': f"Expected 2 updated records, got {actual_count}"
            }
            
            self.test_results.append(result)
            print(f"\nScenario 2 Status: {status}")
            
            return result
            
        except Exception as e:
            error_result = {
                'scenario': 'Update Existing Records',
                'expected_count': 2,
                'actual_count': 0,
                'status': 'FAIL',
                'details': f"Error: {str(e)}"
            }
            self.test_results.append(error_result)
            print(f"\nScenario 2 Status: FAIL - {str(e)}")
            return error_result
    
    def generate_test_report(self):
        """Generate markdown test report"""
        report = "\n" + "="*60 + "\n"
        report += "# MORTGAGE AMENDMENT ETL TEST REPORT\n"
        report += "="*60 + "\n\n"
        
        for i, result in enumerate(self.test_results, 1):
            report += f"## Test Scenario {i}: {result['scenario']}\n\n"
            
            if result['scenario'] == 'Insert New Records':
                report += "### Input Data:\n"
                report += "| event_id | loan_id | event_type |\n"
                report += "|----------|---------|------------|\n"
                report += "| evt_new_001 | loan_new_12345 | MORTGAGE_AMENDMENT |\n"
                report += "| evt_new_002 | loan_new_67890 | MORTGAGE_AMENDMENT |\n\n"
                
                report += "### Expected Output:\n"
                report += "| event_id | loan_id | xml_generated |\n"
                report += "|----------|---------|---------------|\n"
                report += "| evt_new_001 | loan_new_12345 | Yes |\n"
                report += "| evt_new_002 | loan_new_67890 | Yes |\n\n"
                
            elif result['scenario'] == 'Update Existing Records':
                report += "### Input Data (Updates):\n"
                report += "| event_id | loan_id | amendment_type |\n"
                report += "|----------|---------|----------------|\n"
                report += "| evt_update_001 | loan_existing_12345 | RATE_CHANGE |\n"
                report += "| evt_update_002 | loan_existing_67890 | PAYMENT_CHANGE |\n\n"
                
                report += "### Expected Output:\n"
                report += "| event_id | loan_id | xml_updated |\n"
                report += "|----------|---------|-------------|\n"
                report += "| evt_update_001 | loan_existing_12345 | Yes |\n"
                report += "| evt_update_002 | loan_existing_67890 | Yes |\n\n"
            
            report += f"### Results:\n"
            report += f"- **Expected Count**: {result['expected_count']}\n"
            report += f"- **Actual Count**: {result['actual_count']}\n"
            report += f"- **Status**: **{result['status']}**\n"
            report += f"- **Details**: {result['details']}\n\n"
            
            report += "---\n\n"
        
        # Summary
        total_tests = len(self.test_results)
        passed_tests = sum(1 for r in self.test_results if r['status'] == 'PASS')
        
        report += "## Test Summary\n\n"
        report += f"- **Total Tests**: {total_tests}\n"
        report += f"- **Passed**: {passed_tests}\n"
        report += f"- **Failed**: {total_tests - passed_tests}\n"
        report += f"- **Success Rate**: {(passed_tests/total_tests)*100:.1f}%\n\n"
        
        if passed_tests == total_tests:
            report += "🎉 **ALL TESTS PASSED!** 🎉\n"
        else:
            report += "⚠️ **SOME TESTS FAILED** ⚠️\n"
        
        return report
    
    def run_all_tests(self):
        """Run all test scenarios"""
        print("Starting Mortgage Amendment ETL Test Suite...")
        print("=" * 60)
        
        # Run test scenarios
        self.test_scenario_1_insert()
        self.test_scenario_2_update()
        
        # Generate and display report
        report = self.generate_test_report()
        print(report)
        
        return self.test_results

# Main execution
if __name__ == "__main__":
    tester = MortgageAmendmentETLTester()
    test_results = tester.run_all_tests()
    
    print("\n" + "="*60)
    print("TEST EXECUTION COMPLETED")
    print("="*60)
