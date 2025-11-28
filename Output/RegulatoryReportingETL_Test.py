====================================================================
# Author: Ascendion AAVA
# Date: 
# Description: Python-based test script for RegulatoryReportingETL without PyTest framework
====================================================================

import sys
import logging
from datetime import date, datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, when, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType

# Import the main ETL functions
sys.path.append('.')
from RegulatoryReportingETL_ETL import (
    get_spark_session, create_aml_customer_transactions, 
    create_branch_summary_report, write_to_delta_table,
    validate_data_quality
)

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class TestResults:
    """Class to store and manage test results"""
    def __init__(self):
        self.results = []
    
    def add_result(self, test_name, status, input_data, output_data, details=""):
        self.results.append({
            'test_name': test_name,
            'status': status,
            'input_data': input_data,
            'output_data': output_data,
            'details': details
        })
    
    def generate_markdown_report(self):
        """Generate markdown test report"""
        report = "# Test Report\n\n"
        
        for result in self.results:
            report += f"## {result['test_name']}\n\n"
            report += f"**Status: {result['status']}**\n\n"
            
            if result['input_data']:
                report += "### Input Data:\n"
                report += result['input_data'] + "\n\n"
            
            if result['output_data']:
                report += "### Output Data:\n"
                report += result['output_data'] + "\n\n"
            
            if result['details']:
                report += f"**Details:** {result['details']}\n\n"
            
            report += "---\n\n"
        
        return report

def create_test_data_scenario1(spark):
    """Create test data for Scenario 1: Insert new records"""
    logger.info("Creating test data for Scenario 1: Insert")
    
    # Customer data
    customer_schema = StructType([
        StructField("CUSTOMER_ID", IntegerType(), True),
        StructField("NAME", StringType(), True)
    ])
    customer_data = [
        (1, "Alice Johnson"),
        (2, "Bob Smith")
    ]
    customer_df = spark.createDataFrame(customer_data, customer_schema)
    
    # Account data
    account_schema = StructType([
        StructField("ACCOUNT_ID", IntegerType(), True),
        StructField("CUSTOMER_ID", IntegerType(), True),
        StructField("BRANCH_ID", IntegerType(), True)
    ])
    account_data = [
        (201, 1, 10),
        (202, 2, 20)
    ]
    account_df = spark.createDataFrame(account_data, account_schema)
    
    # Transaction data
    transaction_schema = StructType([
        StructField("TRANSACTION_ID", IntegerType(), True),
        StructField("ACCOUNT_ID", IntegerType(), True),
        StructField("AMOUNT", DoubleType(), True),
        StructField("TRANSACTION_TYPE", StringType(), True),
        StructField("TRANSACTION_DATE", DateType(), True)
    ])
    transaction_data = [
        (3001, 201, 1500.0, "DEPOSIT", date(2024, 2, 1)),
        (3002, 202, 750.0, "WITHDRAWAL", date(2024, 2, 2))
    ]
    transaction_df = spark.createDataFrame(transaction_data, transaction_schema)
    
    # Branch data
    branch_schema = StructType([
        StructField("BRANCH_ID", IntegerType(), True),
        StructField("BRANCH_NAME", StringType(), True)
    ])
    branch_data = [
        (10, "Central Branch"),
        (20, "West Branch")
    ]
    branch_df = spark.createDataFrame(branch_data, branch_schema)
    
    # Branch operational data
    branch_operational_schema = StructType([
        StructField("BRANCH_ID", IntegerType(), True),
        StructField("REGION", StringType(), True),
        StructField("MANAGER_NAME", StringType(), True),
        StructField("LAST_AUDIT_DATE", DateType(), True),
        StructField("IS_ACTIVE", StringType(), True)
    ])
    branch_operational_data = [
        (10, "Central Region", "Manager A", date(2024, 1, 15), "Y"),
        (20, "West Region", "Manager B", date(2024, 1, 20), "Y")
    ]
    branch_operational_df = spark.createDataFrame(branch_operational_data, branch_operational_schema)
    
    return customer_df, account_df, transaction_df, branch_df, branch_operational_df

def create_test_data_scenario2(spark):
    """Create test data for Scenario 2: Update existing records"""
    logger.info("Creating test data for Scenario 2: Update")
    
    # Customer data (same IDs as scenario 1 but updated info)
    customer_schema = StructType([
        StructField("CUSTOMER_ID", IntegerType(), True),
        StructField("NAME", StringType(), True)
    ])
    customer_data = [
        (1, "Alice Johnson Updated"),
        (2, "Bob Smith Updated")
    ]
    customer_df = spark.createDataFrame(customer_data, customer_schema)
    
    # Account data (same structure)
    account_schema = StructType([
        StructField("ACCOUNT_ID", IntegerType(), True),
        StructField("CUSTOMER_ID", IntegerType(), True),
        StructField("BRANCH_ID", IntegerType(), True)
    ])
    account_data = [
        (201, 1, 10),
        (202, 2, 20)
    ]
    account_df = spark.createDataFrame(account_data, account_schema)
    
    # Transaction data (updated amounts)
    transaction_schema = StructType([
        StructField("TRANSACTION_ID", IntegerType(), True),
        StructField("ACCOUNT_ID", IntegerType(), True),
        StructField("AMOUNT", DoubleType(), True),
        StructField("TRANSACTION_TYPE", StringType(), True),
        StructField("TRANSACTION_DATE", DateType(), True)
    ])
    transaction_data = [
        (3001, 201, 2000.0, "DEPOSIT", date(2024, 2, 15)),  # Updated amount
        (3002, 202, 1000.0, "WITHDRAWAL", date(2024, 2, 16))  # Updated amount
    ]
    transaction_df = spark.createDataFrame(transaction_data, transaction_schema)
    
    # Branch data (same)
    branch_schema = StructType([
        StructField("BRANCH_ID", IntegerType(), True),
        StructField("BRANCH_NAME", StringType(), True)
    ])
    branch_data = [
        (10, "Central Branch"),
        (20, "West Branch")
    ]
    branch_df = spark.createDataFrame(branch_data, branch_schema)
    
    # Branch operational data (updated audit dates)
    branch_operational_schema = StructType([
        StructField("BRANCH_ID", IntegerType(), True),
        StructField("REGION", StringType(), True),
        StructField("MANAGER_NAME", StringType(), True),
        StructField("LAST_AUDIT_DATE", DateType(), True),
        StructField("IS_ACTIVE", StringType(), True)
    ])
    branch_operational_data = [
        (10, "Central Region Updated", "Manager A Updated", date(2024, 2, 15), "Y"),
        (20, "West Region Updated", "Manager B Updated", date(2024, 2, 20), "Y")
    ]
    branch_operational_df = spark.createDataFrame(branch_operational_data, branch_operational_schema)
    
    return customer_df, account_df, transaction_df, branch_df, branch_operational_df

def dataframe_to_markdown_table(df, table_name):
    """Convert DataFrame to markdown table format"""
    try:
        # Collect data (limit to 10 rows for display)
        rows = df.limit(10).collect()
        if not rows:
            return f"**{table_name}:** No data\n"
        
        # Get column names
        columns = df.columns
        
        # Create markdown table
        markdown = f"**{table_name}:**\n\n"
        markdown += "| " + " | ".join(columns) + " |\n"
        markdown += "| " + " | ".join(["---" for _ in columns]) + " |\n"
        
        for row in rows:
            values = [str(row[col]) if row[col] is not None else "NULL" for col in columns]
            markdown += "| " + " | ".join(values) + " |\n"
        
        markdown += "\n"
        return markdown
    except Exception as e:
        return f"**{table_name}:** Error converting to table - {str(e)}\n"

def test_scenario_1_insert(spark, test_results):
    """Test Scenario 1: Insert new records into target table"""
    logger.info("Running Test Scenario 1: Insert")
    
    try:
        # Create test data
        customer_df, account_df, transaction_df, branch_df, branch_operational_df = create_test_data_scenario1(spark)
        
        # Prepare input data description
        input_desc = "### Source Data:\n"
        input_desc += dataframe_to_markdown_table(customer_df, "CUSTOMER")
        input_desc += dataframe_to_markdown_table(account_df, "ACCOUNT")
        input_desc += dataframe_to_markdown_table(transaction_df, "TRANSACTION")
        input_desc += dataframe_to_markdown_table(branch_df, "BRANCH")
        input_desc += dataframe_to_markdown_table(branch_operational_df, "BRANCH_OPERATIONAL_DETAILS")
        
        # Test AML Customer Transactions
        aml_result = create_aml_customer_transactions(customer_df, account_df, transaction_df)
        
        # Test Enhanced Branch Summary Report
        branch_summary_result = create_branch_summary_report(
            transaction_df, account_df, branch_df, branch_operational_df
        )
        
        # Prepare output data description
        output_desc = "### Generated Output:\n"
        output_desc += dataframe_to_markdown_table(aml_result, "AML_CUSTOMER_TRANSACTIONS")
        output_desc += dataframe_to_markdown_table(branch_summary_result, "BRANCH_SUMMARY_REPORT")
        
        # Validate results
        aml_count = aml_result.count()
        branch_count = branch_summary_result.count()
        
        # Check if REGION and LAST_AUDIT_DATE are populated correctly
        branch_with_region = branch_summary_result.filter(col("REGION").isNotNull()).count()
        
        success = aml_count > 0 and branch_count > 0 and branch_with_region > 0
        status = "PASS" if success else "FAIL"
        
        details = f"AML Records: {aml_count}, Branch Records: {branch_count}, Branches with Region: {branch_with_region}"
        
        test_results.add_result(
            "Scenario 1: Insert New Records",
            status,
            input_desc,
            output_desc,
            details
        )
        
        logger.info(f"Scenario 1 completed with status: {status}")
        return success
        
    except Exception as e:
        logger.error(f"Scenario 1 failed: {str(e)}")
        test_results.add_result(
            "Scenario 1: Insert New Records",
            "FAIL",
            "Test data creation failed",
            "",
            f"Error: {str(e)}"
        )
        return False

def test_scenario_2_update(spark, test_results):
    """Test Scenario 2: Update existing records in target table"""
    logger.info("Running Test Scenario 2: Update")
    
    try:
        # Create updated test data
        customer_df, account_df, transaction_df, branch_df, branch_operational_df = create_test_data_scenario2(spark)
        
        # Prepare input data description
        input_desc = "### Updated Source Data:\n"
        input_desc += dataframe_to_markdown_table(customer_df, "CUSTOMER (Updated)")
        input_desc += dataframe_to_markdown_table(transaction_df, "TRANSACTION (Updated Amounts)")
        input_desc += dataframe_to_markdown_table(branch_operational_df, "BRANCH_OPERATIONAL_DETAILS (Updated)")
        
        # Test AML Customer Transactions with updated data
        aml_result = create_aml_customer_transactions(customer_df, account_df, transaction_df)
        
        # Test Enhanced Branch Summary Report with updated data
        branch_summary_result = create_branch_summary_report(
            transaction_df, account_df, branch_df, branch_operational_df
        )
        
        # Prepare output data description
        output_desc = "### Updated Output:\n"
        output_desc += dataframe_to_markdown_table(aml_result, "AML_CUSTOMER_TRANSACTIONS (Updated)")
        output_desc += dataframe_to_markdown_table(branch_summary_result, "BRANCH_SUMMARY_REPORT (Updated)")
        
        # Validate that updates are reflected
        aml_count = aml_result.count()
        branch_count = branch_summary_result.count()
        
        # Check if updated amounts are reflected
        total_amount = branch_summary_result.agg({"TOTAL_AMOUNT": "sum"}).collect()[0][0]
        expected_total = 3000.0  # 2000 + 1000 from updated amounts
        
        # Check if REGION fields are updated
        updated_regions = branch_summary_result.filter(col("REGION").contains("Updated")).count()
        
        success = (aml_count > 0 and branch_count > 0 and 
                  abs(total_amount - expected_total) < 0.01 and updated_regions > 0)
        status = "PASS" if success else "FAIL"
        
        details = f"AML Records: {aml_count}, Branch Records: {branch_count}, Total Amount: {total_amount}, Updated Regions: {updated_regions}"
        
        test_results.add_result(
            "Scenario 2: Update Existing Records",
            status,
            input_desc,
            output_desc,
            details
        )
        
        logger.info(f"Scenario 2 completed with status: {status}")
        return success
        
    except Exception as e:
        logger.error(f"Scenario 2 failed: {str(e)}")
        test_results.add_result(
            "Scenario 2: Update Existing Records",
            "FAIL",
            "Test data creation failed",
            "",
            f"Error: {str(e)}"
        )
        return False

def test_branch_operational_integration(spark, test_results):
    """Test the new BRANCH_OPERATIONAL_DETAILS integration specifically"""
    logger.info("Running Branch Operational Details Integration Test")
    
    try:
        # Create test data with mixed active/inactive branches
        branch_schema = StructType([
            StructField("BRANCH_ID", IntegerType(), True),
            StructField("BRANCH_NAME", StringType(), True)
        ])
        branch_data = [(30, "Test Branch")]
        branch_df = spark.createDataFrame(branch_data, branch_schema)
        
        # Create operational data with inactive branch
        branch_operational_schema = StructType([
            StructField("BRANCH_ID", IntegerType(), True),
            StructField("REGION", StringType(), True),
            StructField("MANAGER_NAME", StringType(), True),
            StructField("LAST_AUDIT_DATE", DateType(), True),
            StructField("IS_ACTIVE", StringType(), True)
        ])
        branch_operational_data = [
            (30, "Test Region", "Test Manager", date(2024, 1, 1), "N")  # Inactive
        ]
        branch_operational_df = spark.createDataFrame(branch_operational_data, branch_operational_schema)
        
        # Create minimal transaction data
        account_schema = StructType([
            StructField("ACCOUNT_ID", IntegerType(), True),
            StructField("CUSTOMER_ID", IntegerType(), True),
            StructField("BRANCH_ID", IntegerType(), True)
        ])
        account_data = [(301, 1, 30)]
        account_df = spark.createDataFrame(account_data, account_schema)
        
        transaction_schema = StructType([
            StructField("TRANSACTION_ID", IntegerType(), True),
            StructField("ACCOUNT_ID", IntegerType(), True),
            StructField("AMOUNT", DoubleType(), True),
            StructField("TRANSACTION_TYPE", StringType(), True),
            StructField("TRANSACTION_DATE", DateType(), True)
        ])
        transaction_data = [(4001, 301, 100.0, "DEPOSIT", date(2024, 1, 1))]
        transaction_df = spark.createDataFrame(transaction_data, transaction_schema)
        
        # Test the integration
        result = create_branch_summary_report(
            transaction_df, account_df, branch_df, branch_operational_df
        )
        
        # Verify that REGION and LAST_AUDIT_DATE are NULL for inactive branch
        result_row = result.collect()[0]
        region_is_null = result_row['REGION'] is None
        audit_date_is_null = result_row['LAST_AUDIT_DATE'] is None
        
        input_desc = "### Test Data for Inactive Branch:\n"
        input_desc += dataframe_to_markdown_table(branch_operational_df, "BRANCH_OPERATIONAL_DETAILS (IS_ACTIVE=N)")
        
        output_desc = "### Result:\n"
        output_desc += dataframe_to_markdown_table(result, "BRANCH_SUMMARY_REPORT")
        
        success = region_is_null and audit_date_is_null
        status = "PASS" if success else "FAIL"
        
        details = f"Region is NULL: {region_is_null}, Audit Date is NULL: {audit_date_is_null}"
        
        test_results.add_result(
            "Branch Operational Integration Test (Inactive Branch)",
            status,
            input_desc,
            output_desc,
            details
        )
        
        logger.info(f"Branch Operational Integration Test completed with status: {status}")
        return success
        
    except Exception as e:
        logger.error(f"Branch Operational Integration Test failed: {str(e)}")
        test_results.add_result(
            "Branch Operational Integration Test",
            "FAIL",
            "Test setup failed",
            "",
            f"Error: {str(e)}"
        )
        return False

def main():
    """Main test execution function"""
    logger.info("Starting PySpark ETL Test Suite")
    
    spark = None
    test_results = TestResults()
    
    try:
        # Initialize Spark session
        spark = get_spark_session("ETL_Test_Suite")
        logger.info("Spark session initialized for testing")
        
        # Run test scenarios
        test1_passed = test_scenario_1_insert(spark, test_results)
        test2_passed = test_scenario_2_update(spark, test_results)
        test3_passed = test_branch_operational_integration(spark, test_results)
        
        # Generate and print test report
        report = test_results.generate_markdown_report()
        print("\n" + "="*80)
        print("TEST EXECUTION COMPLETED")
        print("="*80)
        print(report)
        
        # Summary
        total_tests = len(test_results.results)
        passed_tests = sum(1 for result in test_results.results if result['status'] == 'PASS')
        
        print(f"\n## Test Summary")
        print(f"**Total Tests:** {total_tests}")
        print(f"**Passed:** {passed_tests}")
        print(f"**Failed:** {total_tests - passed_tests}")
        print(f"**Success Rate:** {(passed_tests/total_tests)*100:.1f}%")
        
        if passed_tests == total_tests:
            print("\n🎉 **ALL TESTS PASSED!**")
        else:
            print("\n❌ **SOME TESTS FAILED!**")
        
        return passed_tests == total_tests
        
    except Exception as e:
        logger.error(f"Test suite failed with exception: {e}")
        print(f"\n❌ **TEST SUITE EXECUTION FAILED:** {str(e)}")
        return False
        
    finally:
        if spark:
            try:
                spark.stop()
                logger.info("Spark session stopped")
            except:
                logger.info("Spark session cleanup completed")

if __name__ == "__main__":
    success = main()
    exit_code = 0 if success else 1
    print(f"\nExiting with code: {exit_code}")
    # sys.exit(exit_code)  # Commented out for Databricks compatibility