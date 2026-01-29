_____________________________________________
## *Author*: AAVA
## *Created on*:   
## *Description*: Test script for Mortgage Amendment to MISMO XML PySpark Pipeline
## *Version*: 1 
## *Updated on*: 
_____________________________________________

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta import *
import sys
import os
import tempfile
from datetime import datetime

# Import the main ETL class
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

class TestMortgageAmendmentETL:
    def __init__(self):
        """Initialize test environment"""
        self.spark = SparkSession.getActiveSession()
        if self.spark is None:
            self.spark = SparkSession.builder \
                .appName("Test_G_MtgAmendment_To_MISMO_XML") \
                .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
                .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
                .getOrCreate()
        
        # Create temporary directory for test outputs
        self.temp_dir = tempfile.mkdtemp()
        self.test_results = []
    
    def create_test_data_scenario1(self):
        """Create test data for Scenario 1: Insert new records"""
        # New records that don't exist in target
        landing_data = [
            ("evt_new_001", "loan_new_001", "MORTGAGE_AMENDMENT", 
             '{"amendment_type":"RATE_CHANGE","effective_date":"2024-02-01","new_rate":"3.75"}', 
             datetime.now()),
            ("evt_new_002", "loan_new_002", "MORTGAGE_AMENDMENT", 
             '{"amendment_type":"TERM_CHANGE","effective_date":"2024-02-02","new_term":"25"}', 
             datetime.now())
        ]
        
        template_data = [
            ("MISMO_TEMPLATE_001", 
             """<?xml version="1.0" encoding="UTF-8"?>
<MISMO_DOCUMENT>
    <LOAN_ID>{{LOAN_ID}}</LOAN_ID>
    <EVENT_ID>{{EVENT_ID}}</EVENT_ID>
    <AMENDMENT_TYPE>{{AMENDMENT_TYPE}}</AMENDMENT_TYPE>
    <EFFECTIVE_DATE>{{EFFECTIVE_DATE}}</EFFECTIVE_DATE>
    <NEW_RATE>{{NEW_RATE}}</NEW_RATE>
</MISMO_DOCUMENT>""")
        ]
        
        return landing_data, template_data
    
    def create_test_data_scenario2(self):
        """Create test data for Scenario 2: Update existing records"""
        # Records with existing keys but updated values
        landing_data = [
            ("evt_update_001", "loan_existing_001", "MORTGAGE_AMENDMENT", 
             '{"amendment_type":"RATE_CHANGE","effective_date":"2024-02-15","new_rate":"4.25"}', 
             datetime.now()),
            ("evt_update_002", "loan_existing_002", "MORTGAGE_AMENDMENT", 
             '{"amendment_type":"RATE_CHANGE","effective_date":"2024-02-16","new_rate":"4.50"}', 
             datetime.now())
        ]
        
        # Existing target data to simulate updates
        existing_target_data = [
            ("evt_old_001", "loan_existing_001", 
             """<?xml version="1.0" encoding="UTF-8"?>
<MISMO_DOCUMENT>
    <LOAN_ID>loan_existing_001</LOAN_ID>
    <EVENT_ID>evt_old_001</EVENT_ID>
    <AMENDMENT_TYPE>RATE_CHANGE</AMENDMENT_TYPE>
    <EFFECTIVE_DATE>2024-01-15</EFFECTIVE_DATE>
    <NEW_RATE>3.50</NEW_RATE>
</MISMO_DOCUMENT>""", 
             datetime(2024, 1, 15)),
            ("evt_old_002", "loan_existing_002", 
             """<?xml version="1.0" encoding="UTF-8"?>
<MISMO_DOCUMENT>
    <LOAN_ID>loan_existing_002</LOAN_ID>
    <EVENT_ID>evt_old_002</EVENT_ID>
    <AMENDMENT_TYPE>RATE_CHANGE</AMENDMENT_TYPE>
    <EFFECTIVE_DATE>2024-01-16</EFFECTIVE_DATE>
    <NEW_RATE>3.75</NEW_RATE>
</MISMO_DOCUMENT>""", 
             datetime(2024, 1, 16))
        ]
        
        template_data = [
            ("MISMO_TEMPLATE_001", 
             """<?xml version="1.0" encoding="UTF-8"?>
<MISMO_DOCUMENT>
    <LOAN_ID>{{LOAN_ID}}</LOAN_ID>
    <EVENT_ID>{{EVENT_ID}}</EVENT_ID>
    <AMENDMENT_TYPE>{{AMENDMENT_TYPE}}</AMENDMENT_TYPE>
    <EFFECTIVE_DATE>{{EFFECTIVE_DATE}}</EFFECTIVE_DATE>
    <NEW_RATE>{{NEW_RATE}}</NEW_RATE>
</MISMO_DOCUMENT>""")
        ]
        
        return landing_data, existing_target_data, template_data
    
    def run_etl_test(self, landing_data, template_data, scenario_name):
        """Run ETL pipeline with test data"""
        try:
            # Import and initialize ETL class
            from G_MtgAmendment_To_MISMO_XML_LLD_PySpark_Code_Pipeline_1 import MortgageAmendmentToMISMOXML
            
            etl_job = MortgageAmendmentToMISMOXML()
            
            # Override the create_sample_data method for testing
            etl_job.create_sample_data = lambda: (landing_data, template_data)
            
            # Run the ETL pipeline
            results = etl_job.run_etl_pipeline()
            
            return results
            
        except Exception as e:
            print(f"Error in {scenario_name}: {str(e)}")
            return None
    
    def validate_scenario1_results(self, results, input_data):
        """Validate Scenario 1: Insert results"""
        test_result = {
            "scenario": "Scenario 1: Insert",
            "status": "FAIL",
            "input_count": len(input_data[0]),
            "output_count": 0,
            "details": ""
        }
        
        try:
            if results and results['xml_output']:
                output_count = results['xml_output'].count()
                expected_count = len(input_data[0])
                
                test_result["output_count"] = output_count
                
                if output_count == expected_count:
                    # Verify XML content contains expected loan IDs
                    xml_df = results['xml_output']
                    loan_ids = [row['loan_id'] for row in xml_df.collect()]
                    expected_loan_ids = ["loan_new_001", "loan_new_002"]
                    
                    if all(loan_id in loan_ids for loan_id in expected_loan_ids):
                        test_result["status"] = "PASS"
                        test_result["details"] = f"Successfully inserted {output_count} records with correct loan IDs"
                    else:
                        test_result["details"] = "Loan IDs mismatch in output"
                else:
                    test_result["details"] = f"Expected {expected_count} records, got {output_count}"
            else:
                test_result["details"] = "No XML output generated"
                
        except Exception as e:
            test_result["details"] = f"Validation error: {str(e)}"
        
        return test_result
    
    def validate_scenario2_results(self, results, input_data, existing_data):
        """Validate Scenario 2: Update results"""
        test_result = {
            "scenario": "Scenario 2: Update",
            "status": "FAIL",
            "input_count": len(input_data[0]),
            "output_count": 0,
            "details": ""
        }
        
        try:
            if results and results['xml_output']:
                output_count = results['xml_output'].count()
                expected_count = len(input_data[0])
                
                test_result["output_count"] = output_count
                
                if output_count == expected_count:
                    # Verify XML content has updated values
                    xml_df = results['xml_output']
                    xml_records = xml_df.collect()
                    
                    # Check if new rates are updated
                    updated_rates_found = False
                    for record in xml_records:
                        if "4.25" in record['xml_content'] or "4.50" in record['xml_content']:
                            updated_rates_found = True
                            break
                    
                    if updated_rates_found:
                        test_result["status"] = "PASS"
                        test_result["details"] = f"Successfully updated {output_count} records with new rates"
                    else:
                        test_result["details"] = "Updated rates not found in XML output"
                else:
                    test_result["details"] = f"Expected {expected_count} records, got {output_count}"
            else:
                test_result["details"] = "No XML output generated"
                
        except Exception as e:
            test_result["details"] = f"Validation error: {str(e)}"
        
        return test_result
    
    def run_all_tests(self):
        """Run all test scenarios"""
        print("\n=== Starting Mortgage Amendment ETL Tests ===")
        
        # Test Scenario 1: Insert
        print("\n--- Running Scenario 1: Insert Test ---")
        scenario1_data = self.create_test_data_scenario1()
        results1 = self.run_etl_test(scenario1_data[0], scenario1_data[1], "Scenario 1")
        test_result1 = self.validate_scenario1_results(results1, scenario1_data)
        self.test_results.append(test_result1)
        
        # Test Scenario 2: Update
        print("\n--- Running Scenario 2: Update Test ---")
        scenario2_data = self.create_test_data_scenario2()
        results2 = self.run_etl_test(scenario2_data[0], scenario2_data[2], "Scenario 2")
        test_result2 = self.validate_scenario2_results(results2, scenario2_data, scenario2_data[1])
        self.test_results.append(test_result2)
        
        # Generate test report
        self.generate_test_report()
    
    def generate_test_report(self):
        """Generate markdown test report"""
        print("\n" + "="*60)
        print("TEST EXECUTION REPORT")
        print("="*60)
        
        for result in self.test_results:
            print(f"\n## {result['scenario']}")
            print(f"\n### Input:")
            if result['scenario'] == "Scenario 1: Insert":
                print("| event_id | loan_id | amendment_type | effective_date | new_rate |")
                print("|----------|---------|----------------|----------------|----------|")
                print("| evt_new_001 | loan_new_001 | RATE_CHANGE | 2024-02-01 | 3.75 |")
                print("| evt_new_002 | loan_new_002 | TERM_CHANGE | 2024-02-02 | - |")
            else:
                print("| event_id | loan_id | amendment_type | effective_date | new_rate |")
                print("|----------|---------|----------------|----------------|----------|")
                print("| evt_update_001 | loan_existing_001 | RATE_CHANGE | 2024-02-15 | 4.25 |")
                print("| evt_update_002 | loan_existing_002 | RATE_CHANGE | 2024-02-16 | 4.50 |")
            
            print(f"\n### Output:")
            print(f"Records Processed: {result['output_count']}")
            print(f"Expected Records: {result['input_count']}")
            
            print(f"\n### Status: **{result['status']}**")
            print(f"\n### Details: {result['details']}")
            print("\n" + "-"*50)
        
        # Summary
        passed_tests = sum(1 for result in self.test_results if result['status'] == 'PASS')
        total_tests = len(self.test_results)
        
        print(f"\n## SUMMARY")
        print(f"Total Tests: {total_tests}")
        print(f"Passed: {passed_tests}")
        print(f"Failed: {total_tests - passed_tests}")
        print(f"Success Rate: {(passed_tests/total_tests)*100:.1f}%")
        print("\n" + "="*60)

# Main execution
if __name__ == "__main__":
    # Initialize and run tests
    test_runner = TestMortgageAmendmentETL()
    test_runner.run_all_tests()
    
    print("\n=== Test Execution Completed ===")
