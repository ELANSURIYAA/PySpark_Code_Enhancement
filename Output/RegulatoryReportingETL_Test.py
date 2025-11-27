====================================================================
# Author: Ascendion AAVA
# Date: <Leave it blank>
# Description: Test script for Enhanced Regulatory Reporting ETL with insert and update scenarios
====================================================================

import logging
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, count, sum, when, lit
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, DateType
from datetime import datetime
import sys

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class ETLTester:
    """
    Test class for validating PySpark ETL functionality with insert and update scenarios.
    """
    
    def __init__(self):
        self.spark = self.get_spark_session()
        self.test_results = []
        
    def get_spark_session(self) -> SparkSession:
        """
        Initialize Spark session for testing.
        """
        try:
            try:
                spark = SparkSession.getActiveSession()
                if spark is None:
                    raise Exception("No active session found")
            except:
                spark = SparkSession.builder \
                    .appName("ETL_Testing") \
                    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
                    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
                    .getOrCreate()
            
            logger.info("Spark session initialized for testing")
            return spark
        except Exception as e:
            logger.error(f"Error creating Spark session: {e}")
            raise
    
    def create_base_data(self):
        """
        Create base data that will be used across test scenarios.
        """
        # Base customer data
        customer_data = [
            (1, "Alice Johnson", "alice@email.com", "111-222-3333", "100 First St", "2023-01-01"),
            (2, "Bob Wilson", "bob@email.com", "444-555-6666", "200 Second Ave", "2023-01-02")
        ]
        customer_df = self.spark.createDataFrame(customer_data, 
            ["CUSTOMER_ID", "NAME", "EMAIL", "PHONE", "ADDRESS", "CREATED_DATE"])
        
        # Base branch data
        branch_data = [
            (201, "Test Branch A", "TB001", "TestCity", "TS", "USA"),
            (202, "Test Branch B", "TB002", "TestTown", "TS", "USA")
        ]
        branch_df = self.spark.createDataFrame(branch_data, 
            ["BRANCH_ID", "BRANCH_NAME", "BRANCH_CODE", "CITY", "STATE", "COUNTRY"])
        
        # Base account data
        account_data = [
            (2001, 1, 201, "TACC001", "SAVINGS", 1000.00, "2023-01-01"),
            (2002, 2, 202, "TACC002", "CHECKING", 2000.00, "2023-01-02")
        ]
        account_df = self.spark.createDataFrame(account_data, 
            ["ACCOUNT_ID", "CUSTOMER_ID", "BRANCH_ID", "ACCOUNT_NUMBER", "ACCOUNT_TYPE", "BALANCE", "OPENED_DATE"])
        
        return customer_df, branch_df, account_df
    
    def test_scenario_1_insert(self):
        """
        Test Scenario 1: Insert new records into target table.
        Tests the ETL with completely new transaction data.
        """
        logger.info("=== Starting Test Scenario 1: INSERT ===")
        
        try:
            # Get base data
            customer_df, branch_df, account_df = self.create_base_data()
            
            # Create NEW transaction data (insert scenario)
            transaction_data = [
                (30001, 2001, "DEPOSIT", 500.00, "2023-03-01", "Test deposit 1"),
                (30002, 2002, "WITHDRAWAL", 200.00, "2023-03-02", "Test withdrawal 1"),
                (30003, 2001, "TRANSFER", 100.00, "2023-03-03", "Test transfer 1")
            ]
            transaction_df = self.spark.createDataFrame(transaction_data, 
                ["TRANSACTION_ID", "ACCOUNT_ID", "TRANSACTION_TYPE", "AMOUNT", "TRANSACTION_DATE", "DESCRIPTION"])
            
            # Create branch operational details (NEW source table)
            branch_operational_data = [
                (201, "North Test Region", "Test Manager A", "2023-12-01", "Y"),
                (202, "South Test Region", "Test Manager B", "2023-11-15", "Y")
            ]
            branch_operational_df = self.spark.createDataFrame(branch_operational_data, 
                ["BRANCH_ID", "REGION", "MANAGER_NAME", "LAST_AUDIT_DATE", "IS_ACTIVE"])
            
            # Execute ETL logic (create_branch_summary_report)
            result_df = self.create_branch_summary_report_test(
                transaction_df, account_df, branch_df, branch_operational_df
            )
            
            # Validate results
            expected_records = 2  # Two branches with transactions
            actual_records = result_df.count()
            
            # Check if all expected columns exist
            expected_columns = ["BRANCH_ID", "BRANCH_NAME", "TOTAL_TRANSACTIONS", "TOTAL_AMOUNT", "REGION", "LAST_AUDIT_DATE"]
            actual_columns = result_df.columns
            
            # Validate data content
            branch_201_data = result_df.filter(col("BRANCH_ID") == 201).collect()
            branch_202_data = result_df.filter(col("BRANCH_ID") == 202).collect()
            
            # Test assertions
            test_passed = True
            test_details = {
                "scenario": "Insert New Records",
                "input_transactions": transaction_data,
                "expected_records": expected_records,
                "actual_records": actual_records,
                "expected_columns": expected_columns,
                "actual_columns": actual_columns,
                "output_data": result_df.collect(),
                "validations": []
            }
            
            # Validation 1: Record count
            if actual_records == expected_records:
                test_details["validations"].append("✓ Record count matches expected")
            else:
                test_details["validations"].append(f"✗ Record count mismatch: expected {expected_records}, got {actual_records}")
                test_passed = False
            
            # Validation 2: Column structure
            if set(expected_columns) == set(actual_columns):
                test_details["validations"].append("✓ All expected columns present")
            else:
                test_details["validations"].append(f"✗ Column mismatch: missing {set(expected_columns) - set(actual_columns)}")
                test_passed = False
            
            # Validation 3: Data content for Branch 201
            if branch_201_data and branch_201_data[0]["TOTAL_TRANSACTIONS"] == 2:  # 2 transactions for branch 201
                test_details["validations"].append("✓ Branch 201 transaction count correct")
            else:
                test_details["validations"].append("✗ Branch 201 transaction count incorrect")
                test_passed = False
            
            # Validation 4: Region data populated correctly
            if branch_201_data and branch_201_data[0]["REGION"] == "North Test Region":
                test_details["validations"].append("✓ Region data populated correctly")
            else:
                test_details["validations"].append("✗ Region data not populated correctly")
                test_passed = False
            
            test_details["status"] = "PASS" if test_passed else "FAIL"
            self.test_results.append(test_details)
            
            logger.info(f"Test Scenario 1 completed: {test_details['status']}")
            return test_details
            
        except Exception as e:
            logger.error(f"Test Scenario 1 failed with exception: {e}")
            error_details = {
                "scenario": "Insert New Records",
                "status": "FAIL",
                "error": str(e),
                "validations": [f"✗ Exception occurred: {e}"]
            }
            self.test_results.append(error_details)
            return error_details
    
    def test_scenario_2_update(self):
        """
        Test Scenario 2: Update existing records in target table.
        Tests the ETL with modified transaction data for existing branches.
        """
        logger.info("=== Starting Test Scenario 2: UPDATE ===")
        
        try:
            # Get base data
            customer_df, branch_df, account_df = self.create_base_data()
            
            # Create UPDATED transaction data (update scenario)
            # Simulating additional transactions for existing branches
            transaction_data = [
                (40001, 2001, "DEPOSIT", 1500.00, "2023-04-01", "Updated deposit 1"),
                (40002, 2002, "WITHDRAWAL", 800.00, "2023-04-02", "Updated withdrawal 1"),
                (40003, 2001, "TRANSFER", 300.00, "2023-04-03", "Updated transfer 1"),
                (40004, 2002, "DEPOSIT", 600.00, "2023-04-04", "Updated deposit 2")
            ]
            transaction_df = self.spark.createDataFrame(transaction_data, 
                ["TRANSACTION_ID", "ACCOUNT_ID", "TRANSACTION_TYPE", "AMOUNT", "TRANSACTION_DATE", "DESCRIPTION"])
            
            # Create updated branch operational details with some changes
            branch_operational_data = [
                (201, "Updated North Region", "New Manager A", "2023-12-15", "Y"),  # Updated region name
                (202, "Updated South Region", "New Manager B", "2023-12-10", "N")   # Changed to inactive
            ]
            branch_operational_df = self.spark.createDataFrame(branch_operational_data, 
                ["BRANCH_ID", "REGION", "MANAGER_NAME", "LAST_AUDIT_DATE", "IS_ACTIVE"])
            
            # Execute ETL logic
            result_df = self.create_branch_summary_report_test(
                transaction_df, account_df, branch_df, branch_operational_df
            )
            
            # Validate results
            expected_records = 2  # Two branches
            actual_records = result_df.count()
            
            # Get specific branch data for validation
            branch_201_data = result_df.filter(col("BRANCH_ID") == 201).collect()
            branch_202_data = result_df.filter(col("BRANCH_ID") == 202).collect()
            
            # Test assertions
            test_passed = True
            test_details = {
                "scenario": "Update Existing Records",
                "input_transactions": transaction_data,
                "expected_records": expected_records,
                "actual_records": actual_records,
                "output_data": result_df.collect(),
                "validations": []
            }
            
            # Validation 1: Record count
            if actual_records == expected_records:
                test_details["validations"].append("✓ Record count matches expected")
            else:
                test_details["validations"].append(f"✗ Record count mismatch: expected {expected_records}, got {actual_records}")
                test_passed = False
            
            # Validation 2: Updated transaction amounts
            if branch_201_data:
                expected_amount_201 = 1500.00 + 300.00  # Two transactions for branch 201
                actual_amount_201 = branch_201_data[0]["TOTAL_AMOUNT"]
                if abs(actual_amount_201 - expected_amount_201) < 0.01:
                    test_details["validations"].append("✓ Branch 201 total amount updated correctly")
                else:
                    test_details["validations"].append(f"✗ Branch 201 amount incorrect: expected {expected_amount_201}, got {actual_amount_201}")
                    test_passed = False
            
            # Validation 3: Updated region name
            if branch_201_data and branch_201_data[0]["REGION"] == "Updated North Region":
                test_details["validations"].append("✓ Branch 201 region updated correctly")
            else:
                test_details["validations"].append("✗ Branch 201 region not updated correctly")
                test_passed = False
            
            # Validation 4: Inactive branch handling (IS_ACTIVE = 'N')
            if branch_202_data and branch_202_data[0]["REGION"] is None:
                test_details["validations"].append("✓ Inactive branch region correctly set to null")
            else:
                test_details["validations"].append("✗ Inactive branch region not handled correctly")
                test_passed = False
            
            test_details["status"] = "PASS" if test_passed else "FAIL"
            self.test_results.append(test_details)
            
            logger.info(f"Test Scenario 2 completed: {test_details['status']}")
            return test_details
            
        except Exception as e:
            logger.error(f"Test Scenario 2 failed with exception: {e}")
            error_details = {
                "scenario": "Update Existing Records",
                "status": "FAIL",
                "error": str(e),
                "validations": [f"✗ Exception occurred: {e}"]
            }
            self.test_results.append(error_details)
            return error_details
    
    def create_branch_summary_report_test(self, transaction_df: DataFrame, account_df: DataFrame, 
                                         branch_df: DataFrame, branch_operational_df: DataFrame) -> DataFrame:
        """
        Test version of the create_branch_summary_report function.
        This replicates the enhanced logic from the main ETL script.
        """
        logger.info("Creating Enhanced Branch Summary Report for testing")
        
        # Base aggregation
        base_summary = transaction_df.join(account_df, "ACCOUNT_ID") \
                                     .join(branch_df, "BRANCH_ID") \
                                     .groupBy("BRANCH_ID", "BRANCH_NAME") \
                                     .agg(
                                         count("*").alias("TOTAL_TRANSACTIONS"),
                                         sum("AMOUNT").alias("TOTAL_AMOUNT")
                                     )
        
        # Integration with BRANCH_OPERATIONAL_DETAILS
        enhanced_summary = base_summary.join(branch_operational_df, "BRANCH_ID", "left")
        
        # Conditional population based on IS_ACTIVE = 'Y'
        final_summary = enhanced_summary.withColumn(
            "REGION", 
            when(col("IS_ACTIVE") == "Y", col("REGION")).otherwise(lit(None))
        ).withColumn(
            "LAST_AUDIT_DATE", 
            when(col("IS_ACTIVE") == "Y", col("LAST_AUDIT_DATE")).otherwise(lit(None))
        ).select(
            col("BRANCH_ID"),
            col("BRANCH_NAME"),
            col("TOTAL_TRANSACTIONS"),
            col("TOTAL_AMOUNT"),
            col("REGION"),
            col("LAST_AUDIT_DATE")
        )
        
        return final_summary
    
    def generate_test_report(self):
        """
        Generate a comprehensive test report in markdown format.
        """
        report = []
        report.append("# Test Report - Enhanced Regulatory Reporting ETL")
        report.append("")
        report.append(f"**Test Execution Date:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        report.append(f"**Total Test Scenarios:** {len(self.test_results)}")
        report.append("")
        
        for i, test in enumerate(self.test_results, 1):
            report.append(f"## Scenario {i}: {test['scenario']}")
            report.append("")
            
            # Input section
            if 'input_transactions' in test:
                report.append("### Input Data:")
                report.append("| Transaction ID | Account ID | Type | Amount | Date | Description |")
                report.append("|----------------|------------|------|--------|------|-------------|")
                for txn in test['input_transactions']:
                    report.append(f"| {txn[0]} | {txn[1]} | {txn[2]} | ${txn[3]:.2f} | {txn[4]} | {txn[5]} |")
                report.append("")
            
            # Output section
            if 'output_data' in test and test['output_data']:
                report.append("### Output Data:")
                report.append("| Branch ID | Branch Name | Total Transactions | Total Amount | Region | Last Audit Date |")
                report.append("|-----------|-------------|-------------------|--------------|--------|-----------------|")
                for row in test['output_data']:
                    region = row['REGION'] if row['REGION'] else 'NULL'
                    audit_date = row['LAST_AUDIT_DATE'] if row['LAST_AUDIT_DATE'] else 'NULL'
                    report.append(f"| {row['BRANCH_ID']} | {row['BRANCH_NAME']} | {row['TOTAL_TRANSACTIONS']} | ${row['TOTAL_AMOUNT']:.2f} | {region} | {audit_date} |")
                report.append("")
            
            # Validations section
            report.append("### Validations:")
            for validation in test['validations']:
                report.append(f"- {validation}")
            report.append("")
            
            # Status
            status_emoji = "✅" if test['status'] == 'PASS' else "❌"
            report.append(f"### Status: {status_emoji} {test['status']}")
            
            if 'error' in test:
                report.append(f"**Error:** {test['error']}")
            
            report.append("")
            report.append("---")
            report.append("")
        
        # Summary
        passed_tests = sum(1 for test in self.test_results if test['status'] == 'PASS')
        failed_tests = len(self.test_results) - passed_tests
        
        report.append("## Test Summary")
        report.append("")
        report.append(f"- **Total Tests:** {len(self.test_results)}")
        report.append(f"- **Passed:** {passed_tests} ✅")
        report.append(f"- **Failed:** {failed_tests} ❌")
        report.append(f"- **Success Rate:** {(passed_tests/len(self.test_results)*100):.1f}%")
        
        return "\n".join(report)
    
    def run_all_tests(self):
        """
        Execute all test scenarios and generate report.
        """
        logger.info("Starting comprehensive ETL testing...")
        
        # Run test scenarios
        self.test_scenario_1_insert()
        self.test_scenario_2_update()
        
        # Generate and print report
        report = self.generate_test_report()
        print("\n" + "="*80)
        print("TEST EXECUTION RESULTS")
        print("="*80)
        print(report)
        
        # Cleanup
        try:
            self.spark.stop()
        except:
            pass
        
        return report

def main():
    """
    Main function to execute all tests.
    """
    try:
        tester = ETLTester()
        report = tester.run_all_tests()
        
        # Return success if all tests passed
        passed_tests = sum(1 for test in tester.test_results if test['status'] == 'PASS')
        if passed_tests == len(tester.test_results):
            logger.info("All tests passed successfully!")
            return 0
        else:
            logger.warning(f"Some tests failed. Passed: {passed_tests}/{len(tester.test_results)}")
            return 1
            
    except Exception as e:
        logger.error(f"Test execution failed: {e}")
        return 1

if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)