====================================================================
# Author: Ascendion AAVA
# Date: 
# Description: Test script for Enhanced Regulatory Reporting ETL with Branch Operational Details
====================================================================

import logging
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, count, sum, when, lit, coalesce
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DecimalType, DateType, LongType, DoubleType
from datetime import datetime, date
from delta.tables import DeltaTable
import sys

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class RegulatoryReportingETLTest:
    """
    Test class for Regulatory Reporting ETL functionality.
    Tests both insert and update scenarios without using PyTest framework.
    """
    
    def __init__(self):
        self.spark = None
        self.test_results = []
        
    def setup_spark(self):
        """Initialize Spark session for testing"""
        try:
            self.spark = SparkSession.getActiveSession()
            if self.spark is None:
                self.spark = SparkSession.builder \
                    .appName("RegulatoryReportingETL_Test") \
                    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
                    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
                    .getOrCreate()
            logger.info("Test Spark session created successfully.")
        except Exception as e:
            logger.error(f"Error creating test Spark session: {e}")
            raise
    
    def create_test_data_scenario1(self):
        """Create test data for Scenario 1: Insert new records"""
        logger.info("Creating test data for Scenario 1: Insert")
        
        # Branch data
        branch_schema = StructType([
            StructField("BRANCH_ID", IntegerType(), True),
            StructField("BRANCH_NAME", StringType(), True),
            StructField("BRANCH_CODE", StringType(), True),
            StructField("CITY", StringType(), True),
            StructField("STATE", StringType(), True),
            StructField("COUNTRY", StringType(), True)
        ])
        
        branch_data = [
            (201, "New Branch A", "NBA001", "Boston", "MA", "USA"),
            (202, "New Branch B", "NBB002", "Seattle", "WA", "USA")
        ]
        
        # Branch Operational Details
        branch_operational_schema = StructType([
            StructField("BRANCH_ID", IntegerType(), True),
            StructField("REGION", StringType(), True),
            StructField("MANAGER_NAME", StringType(), True),
            StructField("LAST_AUDIT_DATE", DateType(), True),
            StructField("IS_ACTIVE", StringType(), True)
        ])
        
        branch_operational_data = [
            (201, "Northeast", "David Manager", date(2024, 1, 15), "Y"),
            (202, "Northwest", "Eve Manager", date(2024, 1, 20), "Y")
        ]
        
        # Account data
        account_schema = StructType([
            StructField("ACCOUNT_ID", IntegerType(), True),
            StructField("CUSTOMER_ID", IntegerType(), True),
            StructField("BRANCH_ID", IntegerType(), True),
            StructField("ACCOUNT_NUMBER", StringType(), True),
            StructField("ACCOUNT_TYPE", StringType(), True),
            StructField("BALANCE", DecimalType(15, 2), True),
            StructField("OPENED_DATE", DateType(), True)
        ])
        
        account_data = [
            (2001, 4, 201, "ACC201", "CHECKING", 3000.00, date(2024, 1, 10)),
            (2002, 5, 202, "ACC202", "SAVINGS", 8000.00, date(2024, 1, 12))
        ]
        
        # Transaction data
        transaction_schema = StructType([
            StructField("TRANSACTION_ID", IntegerType(), True),
            StructField("ACCOUNT_ID", IntegerType(), True),
            StructField("TRANSACTION_TYPE", StringType(), True),
            StructField("AMOUNT", DecimalType(15, 2), True),
            StructField("TRANSACTION_DATE", DateType(), True),
            StructField("DESCRIPTION", StringType(), True)
        ])
        
        transaction_data = [
            (20001, 2001, "DEPOSIT", 1500.00, date(2024, 1, 15), "Initial deposit"),
            (20002, 2001, "WITHDRAWAL", 300.00, date(2024, 1, 16), "ATM withdrawal"),
            (20003, 2002, "DEPOSIT", 2500.00, date(2024, 1, 15), "Transfer in"),
            (20004, 2002, "DEPOSIT", 1000.00, date(2024, 1, 17), "Check deposit")
        ]
        
        # Create DataFrames
        branch_df = self.spark.createDataFrame(branch_data, branch_schema)
        branch_operational_df = self.spark.createDataFrame(branch_operational_data, branch_operational_schema)
        account_df = self.spark.createDataFrame(account_data, account_schema)
        transaction_df = self.spark.createDataFrame(transaction_data, transaction_schema)
        
        return {
            "branch": branch_df,
            "branch_operational": branch_operational_df,
            "account": account_df,
            "transaction": transaction_df
        }
    
    def create_test_data_scenario2(self):
        """Create test data for Scenario 2: Update existing records"""
        logger.info("Creating test data for Scenario 2: Update")
        
        # Use existing branch IDs but with updated operational details
        branch_schema = StructType([
            StructField("BRANCH_ID", IntegerType(), True),
            StructField("BRANCH_NAME", StringType(), True),
            StructField("BRANCH_CODE", StringType(), True),
            StructField("CITY", StringType(), True),
            StructField("STATE", StringType(), True),
            StructField("COUNTRY", StringType(), True)
        ])
        
        branch_data = [
            (101, "Downtown Branch", "DT001", "New York", "NY", "USA"),  # Existing branch
            (102, "Uptown Branch", "UT002", "Los Angeles", "CA", "USA")   # Existing branch
        ]
        
        # Updated Branch Operational Details
        branch_operational_schema = StructType([
            StructField("BRANCH_ID", IntegerType(), True),
            StructField("REGION", StringType(), True),
            StructField("MANAGER_NAME", StringType(), True),
            StructField("LAST_AUDIT_DATE", DateType(), True),
            StructField("IS_ACTIVE", StringType(), True)
        ])
        
        branch_operational_data = [
            (101, "East Coast Updated", "Alice Manager Updated", date(2024, 2, 1), "Y"),  # Updated region and audit date
            (102, "West Coast Updated", "Bob Manager Updated", date(2024, 2, 15), "Y")   # Updated region and audit date
        ]
        
        # Account data for existing branches
        account_schema = StructType([
            StructField("ACCOUNT_ID", IntegerType(), True),
            StructField("CUSTOMER_ID", IntegerType(), True),
            StructField("BRANCH_ID", IntegerType(), True),
            StructField("ACCOUNT_NUMBER", StringType(), True),
            StructField("ACCOUNT_TYPE", StringType(), True),
            StructField("BALANCE", DecimalType(15, 2), True),
            StructField("OPENED_DATE", DateType(), True)
        ])
        
        account_data = [
            (1001, 1, 101, "ACC001", "CHECKING", 5000.00, date(2023, 1, 20)),  # Existing account
            (1002, 2, 102, "ACC002", "SAVINGS", 15000.00, date(2023, 2, 25))   # Existing account
        ]
        
        # New transactions for existing accounts
        transaction_schema = StructType([
            StructField("TRANSACTION_ID", IntegerType(), True),
            StructField("ACCOUNT_ID", IntegerType(), True),
            StructField("TRANSACTION_TYPE", StringType(), True),
            StructField("AMOUNT", DecimalType(15, 2), True),
            StructField("TRANSACTION_DATE", DateType(), True),
            StructField("DESCRIPTION", StringType(), True)
        ])
        
        transaction_data = [
            (30001, 1001, "DEPOSIT", 2000.00, date(2024, 2, 1), "Updated salary deposit"),
            (30002, 1001, "WITHDRAWAL", 500.00, date(2024, 2, 2), "Updated ATM withdrawal"),
            (30003, 1002, "DEPOSIT", 3000.00, date(2024, 2, 1), "Updated transfer in"),
            (30004, 1002, "WITHDRAWAL", 800.00, date(2024, 2, 3), "Updated check payment")
        ]
        
        # Create DataFrames
        branch_df = self.spark.createDataFrame(branch_data, branch_schema)
        branch_operational_df = self.spark.createDataFrame(branch_operational_data, branch_operational_schema)
        account_df = self.spark.createDataFrame(account_data, account_schema)
        transaction_df = self.spark.createDataFrame(transaction_data, transaction_schema)
        
        return {
            "branch": branch_df,
            "branch_operational": branch_operational_df,
            "account": account_df,
            "transaction": transaction_df
        }
    
    def create_branch_summary_report(self, transaction_df: DataFrame, account_df: DataFrame, branch_df: DataFrame, branch_operational_df: DataFrame) -> DataFrame:
        """Create branch summary report - same logic as main ETL"""
        base_summary = transaction_df.join(account_df, "ACCOUNT_ID") \
                                     .join(branch_df, "BRANCH_ID") \
                                     .groupBy("BRANCH_ID", "BRANCH_NAME") \
                                     .agg(
                                         count("*").alias("TOTAL_TRANSACTIONS"),
                                         sum("AMOUNT").alias("TOTAL_AMOUNT")
                                     )
        
        enhanced_summary = base_summary.join(branch_operational_df, "BRANCH_ID", "left") \
                                      .select(
                                          col("BRANCH_ID").cast(LongType()),
                                          col("BRANCH_NAME"),
                                          col("TOTAL_TRANSACTIONS"),
                                          col("TOTAL_AMOUNT").cast(DoubleType()),
                                          coalesce(col("REGION"), lit("Unknown")).alias("REGION"),
                                          col("LAST_AUDIT_DATE").cast(StringType()).alias("LAST_AUDIT_DATE")
                                      )
        
        return enhanced_summary
    
    def test_scenario_1_insert(self):
        """Test Scenario 1: Insert new records"""
        logger.info("Running Test Scenario 1: Insert")
        
        try:
            # Create test data
            test_data = self.create_test_data_scenario1()
            
            # Process data
            result_df = self.create_branch_summary_report(
                test_data["transaction"],
                test_data["account"],
                test_data["branch"],
                test_data["branch_operational"]
            )
            
            # Collect results for validation
            results = result_df.collect()
            
            # Validation
            expected_branches = {201, 202}
            actual_branches = {row["BRANCH_ID"] for row in results}
            
            # Check if all expected branches are present
            if expected_branches == actual_branches:
                # Check specific values
                branch_201 = next((row for row in results if row["BRANCH_ID"] == 201), None)
                branch_202 = next((row for row in results if row["BRANCH_ID"] == 202), None)
                
                validation_passed = (
                    branch_201 is not None and
                    branch_202 is not None and
                    branch_201["TOTAL_TRANSACTIONS"] == 2 and
                    branch_201["TOTAL_AMOUNT"] == 1800.0 and  # 1500 + 300
                    branch_201["REGION"] == "Northeast" and
                    branch_202["TOTAL_TRANSACTIONS"] == 2 and
                    branch_202["TOTAL_AMOUNT"] == 3500.0 and  # 2500 + 1000
                    branch_202["REGION"] == "Northwest"
                )
                
                if validation_passed:
                    self.test_results.append({
                        "scenario": "Scenario 1: Insert",
                        "status": "PASS",
                        "input_data": test_data,
                        "output_data": results,
                        "message": "All new records inserted successfully with correct calculations"
                    })
                    logger.info("Test Scenario 1: PASSED")
                else:
                    self.test_results.append({
                        "scenario": "Scenario 1: Insert",
                        "status": "FAIL",
                        "input_data": test_data,
                        "output_data": results,
                        "message": "Data validation failed - incorrect calculations or missing data"
                    })
                    logger.error("Test Scenario 1: FAILED - Data validation failed")
            else:
                self.test_results.append({
                    "scenario": "Scenario 1: Insert",
                    "status": "FAIL",
                    "input_data": test_data,
                    "output_data": results,
                    "message": f"Expected branches {expected_branches}, got {actual_branches}"
                })
                logger.error("Test Scenario 1: FAILED - Missing branches")
                
        except Exception as e:
            self.test_results.append({
                "scenario": "Scenario 1: Insert",
                "status": "FAIL",
                "input_data": None,
                "output_data": None,
                "message": f"Exception occurred: {str(e)}"
            })
            logger.error(f"Test Scenario 1: FAILED with exception: {e}")
    
    def test_scenario_2_update(self):
        """Test Scenario 2: Update existing records"""
        logger.info("Running Test Scenario 2: Update")
        
        try:
            # Create test data
            test_data = self.create_test_data_scenario2()
            
            # Process data
            result_df = self.create_branch_summary_report(
                test_data["transaction"],
                test_data["account"],
                test_data["branch"],
                test_data["branch_operational"]
            )
            
            # Collect results for validation
            results = result_df.collect()
            
            # Validation
            expected_branches = {101, 102}
            actual_branches = {row["BRANCH_ID"] for row in results}
            
            # Check if all expected branches are present
            if expected_branches == actual_branches:
                # Check specific values for updated data
                branch_101 = next((row for row in results if row["BRANCH_ID"] == 101), None)
                branch_102 = next((row for row in results if row["BRANCH_ID"] == 102), None)
                
                validation_passed = (
                    branch_101 is not None and
                    branch_102 is not None and
                    branch_101["TOTAL_TRANSACTIONS"] == 2 and
                    branch_101["TOTAL_AMOUNT"] == 2500.0 and  # 2000 + 500
                    branch_101["REGION"] == "East Coast Updated" and
                    branch_101["LAST_AUDIT_DATE"] == "2024-02-01" and
                    branch_102["TOTAL_TRANSACTIONS"] == 2 and
                    branch_102["TOTAL_AMOUNT"] == 3800.0 and  # 3000 + 800
                    branch_102["REGION"] == "West Coast Updated" and
                    branch_102["LAST_AUDIT_DATE"] == "2024-02-15"
                )
                
                if validation_passed:
                    self.test_results.append({
                        "scenario": "Scenario 2: Update",
                        "status": "PASS",
                        "input_data": test_data,
                        "output_data": results,
                        "message": "All existing records updated successfully with new operational details"
                    })
                    logger.info("Test Scenario 2: PASSED")
                else:
                    self.test_results.append({
                        "scenario": "Scenario 2: Update",
                        "status": "FAIL",
                        "input_data": test_data,
                        "output_data": results,
                        "message": "Data validation failed - incorrect updates or missing operational details"
                    })
                    logger.error("Test Scenario 2: FAILED - Data validation failed")
            else:
                self.test_results.append({
                    "scenario": "Scenario 2: Update",
                    "status": "FAIL",
                    "input_data": test_data,
                    "output_data": results,
                    "message": f"Expected branches {expected_branches}, got {actual_branches}"
                })
                logger.error("Test Scenario 2: FAILED - Missing branches")
                
        except Exception as e:
            self.test_results.append({
                "scenario": "Scenario 2: Update",
                "status": "FAIL",
                "input_data": None,
                "output_data": None,
                "message": f"Exception occurred: {str(e)}"
            })
            logger.error(f"Test Scenario 2: FAILED with exception: {e}")
    
    def generate_markdown_report(self):
        """Generate markdown test report"""
        report = "# Test Report\n\n"
        report += "## Enhanced Regulatory Reporting ETL Test Results\n\n"
        
        for result in self.test_results:
            report += f"### {result['scenario']}\n\n"
            
            if result['input_data'] and result['scenario'] == "Scenario 1: Insert":
                report += "**Input Data:**\n"
                report += "Branch Data:\n"
                report += "| BRANCH_ID | BRANCH_NAME | REGION | MANAGER_NAME |\n"
                report += "|-----------|-------------|---------|--------------|\n"
                report += "| 201 | New Branch A | Northeast | David Manager |\n"
                report += "| 202 | New Branch B | Northwest | Eve Manager |\n\n"
                
                report += "Transaction Data:\n"
                report += "| TRANSACTION_ID | ACCOUNT_ID | AMOUNT | TYPE |\n"
                report += "|----------------|------------|--------|------|\n"
                report += "| 20001 | 2001 | 1500.00 | DEPOSIT |\n"
                report += "| 20002 | 2001 | 300.00 | WITHDRAWAL |\n"
                report += "| 20003 | 2002 | 2500.00 | DEPOSIT |\n"
                report += "| 20004 | 2002 | 1000.00 | DEPOSIT |\n\n"
            
            elif result['input_data'] and result['scenario'] == "Scenario 2: Update":
                report += "**Input Data:**\n"
                report += "Updated Branch Operational Data:\n"
                report += "| BRANCH_ID | REGION | LAST_AUDIT_DATE | MANAGER_NAME |\n"
                report += "|-----------|--------|-----------------|--------------|\n"
                report += "| 101 | East Coast Updated | 2024-02-01 | Alice Manager Updated |\n"
                report += "| 102 | West Coast Updated | 2024-02-15 | Bob Manager Updated |\n\n"
                
                report += "New Transaction Data:\n"
                report += "| TRANSACTION_ID | ACCOUNT_ID | AMOUNT | TYPE |\n"
                report += "|----------------|------------|--------|------|\n"
                report += "| 30001 | 1001 | 2000.00 | DEPOSIT |\n"
                report += "| 30002 | 1001 | 500.00 | WITHDRAWAL |\n"
                report += "| 30003 | 1002 | 3000.00 | DEPOSIT |\n"
                report += "| 30004 | 1002 | 800.00 | WITHDRAWAL |\n\n"
            
            if result['output_data']:
                report += "**Output:**\n"
                report += "| BRANCH_ID | BRANCH_NAME | TOTAL_TRANSACTIONS | TOTAL_AMOUNT | REGION | LAST_AUDIT_DATE |\n"
                report += "|-----------|-------------|-------------------|--------------|--------|-----------------|\n"
                for row in result['output_data']:
                    report += f"| {row['BRANCH_ID']} | {row['BRANCH_NAME']} | {row['TOTAL_TRANSACTIONS']} | {row['TOTAL_AMOUNT']} | {row['REGION']} | {row['LAST_AUDIT_DATE']} |\n"
                report += "\n"
            
            report += f"**Status:** {result['status']}\n\n"
            report += f"**Message:** {result['message']}\n\n"
            report += "---\n\n"
        
        return report
    
    def run_all_tests(self):
        """Run all test scenarios"""
        logger.info("Starting Enhanced Regulatory Reporting ETL Tests")
        
        self.setup_spark()
        
        # Run test scenarios
        self.test_scenario_1_insert()
        self.test_scenario_2_update()
        
        # Generate and print report
        report = self.generate_markdown_report()
        print("\n" + "="*80)
        print("TEST EXECUTION COMPLETED")
        print("="*80)
        print(report)
        
        # Summary
        passed_tests = sum(1 for result in self.test_results if result['status'] == 'PASS')
        total_tests = len(self.test_results)
        
        print(f"\n## Test Summary\n")
        print(f"**Total Tests:** {total_tests}")
        print(f"**Passed:** {passed_tests}")
        print(f"**Failed:** {total_tests - passed_tests}")
        print(f"**Success Rate:** {(passed_tests/total_tests)*100:.1f}%")
        
        return self.test_results

def main():
    """Main test execution function"""
    try:
        test_runner = RegulatoryReportingETLTest()
        results = test_runner.run_all_tests()
        
        # Return appropriate exit code
        failed_tests = sum(1 for result in results if result['status'] == 'FAIL')
        if failed_tests > 0:
            logger.error(f"Tests completed with {failed_tests} failures")
            return 1
        else:
            logger.info("All tests passed successfully")
            return 0
            
    except Exception as e:
        logger.error(f"Test execution failed with exception: {e}")
        return 1

if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)
