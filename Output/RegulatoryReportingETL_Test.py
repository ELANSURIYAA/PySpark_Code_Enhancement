# ====================================================================
# Author: Ascendion AAVA
# Date: 
# Description: Python test script for RegulatoryReportingETL validation with insert and update scenarios
# ====================================================================

import logging
import os
import shutil
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, count, sum as spark_sum, when, lit, current_date
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DecimalType, DateType, LongType, DoubleType
from delta.tables import DeltaTable
from datetime import datetime

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class RegulatoryReportingETLTest:
    """
    Test class for validating RegulatoryReportingETL functionality
    Tests both insert and update scenarios for the branch summary report
    """
    
    def __init__(self):
        self.spark = None
        self.test_results = []
        self.base_path = "/tmp/delta_test"
        
    def setup_spark(self):
        """Initialize Spark session for testing"""
        try:
            self.spark = SparkSession.builder \
                .appName("RegulatoryReportingETL_Test") \
                .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
                .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
                .getOrCreate()
            
            self.spark.conf.set("spark.sql.adaptive.enabled", "true")
            logger.info("Test Spark session created successfully")
            return True
        except Exception as e:
            logger.error(f"Failed to create Spark session: {e}")
            return False
    
    def cleanup_test_data(self):
        """Clean up test directories"""
        try:
            if os.path.exists(self.base_path):
                shutil.rmtree(self.base_path)
            logger.info("Test data cleanup completed")
        except Exception as e:
            logger.warning(f"Cleanup warning: {e}")
    
    def create_base_test_data(self):
        """Create base test data for scenarios"""
        logger.info("Creating base test data")
        
        # Base BRANCH data
        branch_schema = StructType([
            StructField("BRANCH_ID", IntegerType(), True),
            StructField("BRANCH_NAME", StringType(), True),
            StructField("BRANCH_CODE", StringType(), True),
            StructField("CITY", StringType(), True),
            StructField("STATE", StringType(), True),
            StructField("COUNTRY", StringType(), True)
        ])
        
        branch_data = [
            (101, "Downtown Branch", "DT001", "New York", "NY", "USA"),
            (102, "Uptown Branch", "UT002", "Los Angeles", "CA", "USA")
        ]
        
        # Base ACCOUNT data
        account_schema = StructType([
            StructField("ACCOUNT_ID", IntegerType(), True),
            StructField("CUSTOMER_ID", IntegerType(), True),
            StructField("BRANCH_ID", IntegerType(), True),
            StructField("ACCOUNT_NUMBER", StringType(), True),
            StructField("ACCOUNT_TYPE", StringType(), True),
            StructField("BALANCE", DecimalType(15,2), True),
            StructField("OPENED_DATE", DateType(), True)
        ])
        
        account_data = [
            (1001, 1, 101, "ACC001", "SAVINGS", 5000.00, None),
            (1002, 2, 102, "ACC002", "CHECKING", 3000.00, None)
        ]
        
        # Base TRANSACTION data
        transaction_schema = StructType([
            StructField("TRANSACTION_ID", IntegerType(), True),
            StructField("ACCOUNT_ID", IntegerType(), True),
            StructField("TRANSACTION_TYPE", StringType(), True),
            StructField("AMOUNT", DecimalType(15,2), True),
            StructField("TRANSACTION_DATE", DateType(), True),
            StructField("DESCRIPTION", StringType(), True)
        ])
        
        transaction_data = [
            (10001, 1001, "DEPOSIT", 1000.00, None, "Cash deposit"),
            (10002, 1002, "WITHDRAWAL", 500.00, None, "ATM withdrawal")
        ]
        
        # Base BRANCH_OPERATIONAL_DETAILS data
        branch_operational_schema = StructType([
            StructField("BRANCH_ID", IntegerType(), True),
            StructField("REGION", StringType(), True),
            StructField("MANAGER_NAME", StringType(), True),
            StructField("LAST_AUDIT_DATE", DateType(), True),
            StructField("IS_ACTIVE", StringType(), True)
        ])
        
        branch_operational_data = [
            (101, "Northeast", "Alice Manager", None, "Y"),
            (102, "West", "Bob Manager", None, "Y")
        ]
        
        # Create DataFrames
        branch_df = self.spark.createDataFrame(branch_data, branch_schema)
        account_df = self.spark.createDataFrame(account_data, account_schema).withColumn("OPENED_DATE", current_date())
        transaction_df = self.spark.createDataFrame(transaction_data, transaction_schema).withColumn("TRANSACTION_DATE", current_date())
        branch_operational_df = self.spark.createDataFrame(branch_operational_data, branch_operational_schema).withColumn("LAST_AUDIT_DATE", current_date())
        
        return branch_df, account_df, transaction_df, branch_operational_df
    
    def create_branch_summary_report(self, transaction_df, account_df, branch_df, branch_operational_df):
        """Create branch summary report with operational details integration"""
        try:
            # Base aggregation
            base_summary = transaction_df.join(account_df, "ACCOUNT_ID", "inner") \
                                       .join(branch_df, "BRANCH_ID", "inner") \
                                       .groupBy("BRANCH_ID", "BRANCH_NAME") \
                                       .agg(
                                           count("*").alias("TOTAL_TRANSACTIONS"),
                                           spark_sum("AMOUNT").alias("TOTAL_AMOUNT")
                                       )
            
            # Enhanced with operational details
            enhanced_summary = base_summary.join(branch_operational_df, "BRANCH_ID", "left") \
                                         .withColumn(
                                             "REGION", 
                                             when(col("IS_ACTIVE") == "Y", col("REGION")).otherwise(lit(None))
                                         ) \
                                         .withColumn(
                                             "LAST_AUDIT_DATE", 
                                             when(col("IS_ACTIVE") == "Y", col("LAST_AUDIT_DATE")).otherwise(lit(None))
                                         ) \
                                         .select(
                                             col("BRANCH_ID").cast(LongType()),
                                             col("BRANCH_NAME"),
                                             col("TOTAL_TRANSACTIONS"),
                                             col("TOTAL_AMOUNT").cast(DoubleType()),
                                             col("REGION"),
                                             col("LAST_AUDIT_DATE")
                                         )
            
            return enhanced_summary
            
        except Exception as e:
            logger.error(f"Error creating branch summary report: {e}")
            raise
    
    def test_scenario_1_insert(self):
        """Test Scenario 1: Insert new records into target table"""
        logger.info("=== Testing Scenario 1: Insert Operations ===")
        
        try:
            # Create base test data
            branch_df, account_df, transaction_df, branch_operational_df = self.create_base_test_data()
            
            # Create branch summary report
            branch_summary_df = self.create_branch_summary_report(
                transaction_df, account_df, branch_df, branch_operational_df
            )
            
            # Write to Delta table (initial insert)
            target_path = f"{self.base_path}/branch_summary_insert_test"
            branch_summary_df.write.format("delta").mode("overwrite").save(target_path)
            
            # Read back and validate
            result_df = self.spark.read.format("delta").load(target_path)
            result_count = result_df.count()
            
            # Collect results for reporting
            input_data = branch_summary_df.collect()
            output_data = result_df.collect()
            
            # Validation
            expected_count = 2  # We have 2 branches in test data
            test_passed = result_count == expected_count
            
            # Validate data integrity
            for row in output_data:
                if row['BRANCH_ID'] == 101:
                    test_passed = test_passed and row['REGION'] == 'Northeast'
                elif row['BRANCH_ID'] == 102:
                    test_passed = test_passed and row['REGION'] == 'West'
            
            self.test_results.append({
                'scenario': 'Scenario 1: Insert',
                'input_data': input_data,
                'output_data': output_data,
                'expected_count': expected_count,
                'actual_count': result_count,
                'status': 'PASS' if test_passed else 'FAIL',
                'details': f"Expected {expected_count} records, got {result_count}"
            })
            
            logger.info(f"Scenario 1 completed: {'PASS' if test_passed else 'FAIL'}")
            return test_passed
            
        except Exception as e:
            logger.error(f"Scenario 1 failed with error: {e}")
            self.test_results.append({
                'scenario': 'Scenario 1: Insert',
                'input_data': [],
                'output_data': [],
                'expected_count': 2,
                'actual_count': 0,
                'status': 'FAIL',
                'details': f"Test failed with exception: {str(e)}"
            })
            return False
    
    def test_scenario_2_update(self):
        """Test Scenario 2: Update existing records in target table"""
        logger.info("=== Testing Scenario 2: Update Operations ===")
        
        try:
            # First, create initial data (same as scenario 1)
            branch_df, account_df, transaction_df, branch_operational_df = self.create_base_test_data()
            initial_summary_df = self.create_branch_summary_report(
                transaction_df, account_df, branch_df, branch_operational_df
            )
            
            # Write initial data
            target_path = f"{self.base_path}/branch_summary_update_test"
            initial_summary_df.write.format("delta").mode("overwrite").save(target_path)
            
            # Create updated transaction data (additional transactions for existing branches)
            updated_transaction_schema = StructType([
                StructField("TRANSACTION_ID", IntegerType(), True),
                StructField("ACCOUNT_ID", IntegerType(), True),
                StructField("TRANSACTION_TYPE", StringType(), True),
                StructField("AMOUNT", DecimalType(15,2), True),
                StructField("TRANSACTION_DATE", DateType(), True),
                StructField("DESCRIPTION", StringType(), True)
            ])
            
            # Additional transactions for existing accounts
            updated_transaction_data = [
                (10001, 1001, "DEPOSIT", 1000.00, None, "Cash deposit"),  # Original
                (10002, 1002, "WITHDRAWAL", 500.00, None, "ATM withdrawal"),  # Original
                (10003, 1001, "DEPOSIT", 2000.00, None, "New deposit"),  # New transaction
                (10004, 1002, "TRANSFER", 1500.00, None, "Wire transfer")  # New transaction
            ]
            
            updated_transaction_df = self.spark.createDataFrame(updated_transaction_data, updated_transaction_schema) \
                                               .withColumn("TRANSACTION_DATE", current_date())
            
            # Create updated branch summary with new transaction data
            updated_summary_df = self.create_branch_summary_report(
                updated_transaction_df, account_df, branch_df, branch_operational_df
            )
            
            # Perform merge/upsert operation
            delta_table = DeltaTable.forPath(self.spark, target_path)
            
            delta_table.alias("target").merge(
                updated_summary_df.alias("source"),
                "target.BRANCH_ID = source.BRANCH_ID"
            ).whenMatchedUpdateAll() \
             .whenNotMatchedInsertAll() \
             .execute()
            
            # Read back and validate
            result_df = self.spark.read.format("delta").load(target_path)
            result_count = result_df.count()
            
            # Collect results for reporting
            input_data = updated_summary_df.collect()
            output_data = result_df.collect()
            
            # Validation - check if amounts were updated correctly
            test_passed = result_count == 2  # Still 2 branches
            
            for row in output_data:
                if row['BRANCH_ID'] == 101:
                    # Should have 2 transactions now (1000 + 2000 = 3000)
                    test_passed = test_passed and row['TOTAL_TRANSACTIONS'] == 2
                    test_passed = test_passed and row['TOTAL_AMOUNT'] == 3000.0
                elif row['BRANCH_ID'] == 102:
                    # Should have 2 transactions now (500 + 1500 = 2000)
                    test_passed = test_passed and row['TOTAL_TRANSACTIONS'] == 2
                    test_passed = test_passed and row['TOTAL_AMOUNT'] == 2000.0
            
            self.test_results.append({
                'scenario': 'Scenario 2: Update',
                'input_data': input_data,
                'output_data': output_data,
                'expected_count': 2,
                'actual_count': result_count,
                'status': 'PASS' if test_passed else 'FAIL',
                'details': f"Expected updated amounts - Branch 101: 3000.0, Branch 102: 2000.0"
            })
            
            logger.info(f"Scenario 2 completed: {'PASS' if test_passed else 'FAIL'}")
            return test_passed
            
        except Exception as e:
            logger.error(f"Scenario 2 failed with error: {e}")
            self.test_results.append({
                'scenario': 'Scenario 2: Update',
                'input_data': [],
                'output_data': [],
                'expected_count': 2,
                'actual_count': 0,
                'status': 'FAIL',
                'details': f"Test failed with exception: {str(e)}"
            })
            return False
    
    def generate_test_report(self):
        """Generate markdown test report"""
        logger.info("Generating test report")
        
        report = "# Test Report\n\n"
        report += f"**Test Execution Date:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n"
        report += "**Test Summary:**\n\n"
        
        for result in self.test_results:
            report += f"## {result['scenario']}\n\n"
            
            # Input section
            report += "### Input Data:\n"
            if result['input_data']:
                report += "| BRANCH_ID | BRANCH_NAME | TOTAL_TRANSACTIONS | TOTAL_AMOUNT | REGION | LAST_AUDIT_DATE |\n"
                report += "|-----------|-------------|-------------------|--------------|--------|-----------------|\n"
                for row in result['input_data']:
                    audit_date = row['LAST_AUDIT_DATE'].strftime('%Y-%m-%d') if row['LAST_AUDIT_DATE'] else 'NULL'
                    report += f"| {row['BRANCH_ID']} | {row['BRANCH_NAME']} | {row['TOTAL_TRANSACTIONS']} | {row['TOTAL_AMOUNT']} | {row['REGION'] or 'NULL'} | {audit_date} |\n"
            else:
                report += "No input data available\n"
            
            report += "\n"
            
            # Output section
            report += "### Output Data:\n"
            if result['output_data']:
                report += "| BRANCH_ID | BRANCH_NAME | TOTAL_TRANSACTIONS | TOTAL_AMOUNT | REGION | LAST_AUDIT_DATE |\n"
                report += "|-----------|-------------|-------------------|--------------|--------|-----------------|\n"
                for row in result['output_data']:
                    audit_date = row['LAST_AUDIT_DATE'].strftime('%Y-%m-%d') if row['LAST_AUDIT_DATE'] else 'NULL'
                    report += f"| {row['BRANCH_ID']} | {row['BRANCH_NAME']} | {row['TOTAL_TRANSACTIONS']} | {row['TOTAL_AMOUNT']} | {row['REGION'] or 'NULL'} | {audit_date} |\n"
            else:
                report += "No output data available\n"
            
            report += "\n"
            
            # Status and details
            report += f"### Status: **{result['status']}**\n\n"
            report += f"**Details:** {result['details']}\n\n"
            report += "---\n\n"
        
        # Overall summary
        total_tests = len(self.test_results)
        passed_tests = len([r for r in self.test_results if r['status'] == 'PASS'])
        
        report += "## Overall Test Summary\n\n"
        report += f"- **Total Tests:** {total_tests}\n"
        report += f"- **Passed:** {passed_tests}\n"
        report += f"- **Failed:** {total_tests - passed_tests}\n"
        report += f"- **Success Rate:** {(passed_tests/total_tests)*100:.1f}%\n\n"
        
        return report
    
    def run_all_tests(self):
        """Run all test scenarios"""
        logger.info("Starting RegulatoryReportingETL Test Suite")
        
        # Setup
        if not self.setup_spark():
            return False
        
        self.cleanup_test_data()
        
        try:
            # Run test scenarios
            scenario1_result = self.test_scenario_1_insert()
            scenario2_result = self.test_scenario_2_update()
            
            # Generate and print report
            report = self.generate_test_report()
            print("\n" + "="*80)
            print("TEST EXECUTION RESULTS")
            print("="*80)
            print(report)
            
            return scenario1_result and scenario2_result
            
        except Exception as e:
            logger.error(f"Test suite failed: {e}")
            return False
        finally:
            self.cleanup_test_data()
            if self.spark:
                try:
                    self.spark.stop()
                except:
                    pass

def main():
    """Main test execution function"""
    test_suite = RegulatoryReportingETLTest()
    success = test_suite.run_all_tests()
    
    if success:
        logger.info("All tests passed successfully!")
        return 0
    else:
        logger.error("Some tests failed!")
        return 1

if __name__ == "__main__":
    exit_code = main()
    exit(exit_code)