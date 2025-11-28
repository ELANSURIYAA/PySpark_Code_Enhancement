====================================================================
# Author: Ascendion AAVA
# Date: 
# Description: Python test script for RegulatoryReportingETL Pipeline - Tests insert and update scenarios
====================================================================

import logging
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, count, sum, when, lit, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType, LongType
from datetime import datetime, date
import tempfile
import shutil
import os

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class RegulatoryReportingETLTest:
    """
    Test class for RegulatoryReportingETL Pipeline
    Tests both insert and update scenarios without using PyTest framework
    """
    
    def __init__(self):
        self.spark = None
        self.temp_dir = None
        self.test_results = []
        
    def setup(self):
        """Setup test environment"""
        try:
            # Create Spark session
            self.spark = SparkSession.builder \
                .appName("RegulatoryReportingETL_Test") \
                .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
                .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
                .master("local[*]") \
                .getOrCreate()
            
            # Create temporary directory for Delta tables
            self.temp_dir = tempfile.mkdtemp()
            logger.info(f"Test setup completed. Temp directory: {self.temp_dir}")
            return True
        except Exception as e:
            logger.error(f"Test setup failed: {e}")
            return False
    
    def teardown(self):
        """Cleanup test environment"""
        try:
            if self.spark:
                self.spark.stop()
            if self.temp_dir and os.path.exists(self.temp_dir):
                shutil.rmtree(self.temp_dir)
            logger.info("Test teardown completed")
        except Exception as e:
            logger.error(f"Test teardown failed: {e}")
    
    def create_initial_data(self):
        """Create initial sample data for testing"""
        # Customer data
        customer_schema = StructType([
            StructField("CUSTOMER_ID", IntegerType(), True),
            StructField("NAME", StringType(), True)
        ])
        customer_data = [(1, "John Doe"), (2, "Jane Smith")]
        customer_df = self.spark.createDataFrame(customer_data, customer_schema)
        
        # Account data
        account_schema = StructType([
            StructField("ACCOUNT_ID", IntegerType(), True),
            StructField("CUSTOMER_ID", IntegerType(), True),
            StructField("BRANCH_ID", IntegerType(), True)
        ])
        account_data = [(101, 1, 1), (102, 2, 1)]
        account_df = self.spark.createDataFrame(account_data, account_schema)
        
        # Transaction data
        transaction_schema = StructType([
            StructField("TRANSACTION_ID", IntegerType(), True),
            StructField("ACCOUNT_ID", IntegerType(), True),
            StructField("AMOUNT", DoubleType(), True),
            StructField("TRANSACTION_TYPE", StringType(), True),
            StructField("TRANSACTION_DATE", DateType(), True)
        ])
        transaction_data = [
            (1001, 101, 1000.0, "DEPOSIT", date(2024, 1, 15)),
            (1002, 102, 500.0, "WITHDRAWAL", date(2024, 1, 16))
        ]
        transaction_df = self.spark.createDataFrame(transaction_data, transaction_schema)
        
        # Branch data
        branch_schema = StructType([
            StructField("BRANCH_ID", IntegerType(), True),
            StructField("BRANCH_NAME", StringType(), True)
        ])
        branch_data = [(1, "Downtown Branch")]
        branch_df = self.spark.createDataFrame(branch_data, branch_schema)
        
        # Branch Operational Details
        branch_operational_schema = StructType([
            StructField("BRANCH_ID", IntegerType(), True),
            StructField("REGION", StringType(), True),
            StructField("MANAGER_NAME", StringType(), True),
            StructField("LAST_AUDIT_DATE", DateType(), True),
            StructField("IS_ACTIVE", StringType(), True)
        ])
        branch_operational_data = [(1, "North Region", "Alice Manager", date(2024, 1, 10), "Y")]
        branch_operational_df = self.spark.createDataFrame(branch_operational_data, branch_operational_schema)
        
        return {
            "customer": customer_df,
            "account": account_df,
            "transaction": transaction_df,
            "branch": branch_df,
            "branch_operational": branch_operational_df
        }
    
    def create_branch_summary_report(self, transaction_df, account_df, branch_df, branch_operational_df):
        """Create branch summary report with operational details integration"""
        # Base aggregation
        base_summary = transaction_df.join(account_df, "ACCOUNT_ID") \
                                     .join(branch_df, "BRANCH_ID") \
                                     .groupBy("BRANCH_ID", "BRANCH_NAME") \
                                     .agg(
                                         count("*").alias("TOTAL_TRANSACTIONS"),
                                         sum("AMOUNT").alias("TOTAL_AMOUNT")
                                     )
        
        # Enhanced with operational details
        enhanced_summary = base_summary.join(branch_operational_df, "BRANCH_ID", "left") \
                                      .select(
                                          col("BRANCH_ID"),
                                          col("BRANCH_NAME"),
                                          col("TOTAL_TRANSACTIONS"),
                                          col("TOTAL_AMOUNT"),
                                          when(col("IS_ACTIVE") == "Y", col("REGION")).alias("REGION"),
                                          when(col("IS_ACTIVE") == "Y", col("LAST_AUDIT_DATE")).alias("LAST_AUDIT_DATE")
                                      )
        
        return enhanced_summary
    
    def test_scenario_1_insert(self):
        """Test Scenario 1: Insert new records into target table"""
        logger.info("=== Starting Test Scenario 1: Insert ===")
        
        try:
            # Create initial data
            initial_data = self.create_initial_data()
            
            # Create branch summary report
            branch_summary_df = self.create_branch_summary_report(
                initial_data["transaction"],
                initial_data["account"],
                initial_data["branch"],
                initial_data["branch_operational"]
            )
            
            # Write to Delta table
            target_path = f"{self.temp_dir}/branch_summary_insert"
            branch_summary_df.write.format("delta").mode("overwrite").save(target_path)
            
            # Read back and validate
            result_df = self.spark.read.format("delta").load(target_path)
            result_count = result_df.count()
            
            # Collect results for reporting
            input_data = initial_data["transaction"].collect()
            output_data = result_df.collect()
            
            # Validation
            expected_count = 1  # One branch
            test_passed = result_count == expected_count
            
            self.test_results.append({
                "scenario": "Scenario 1: Insert",
                "input_data": input_data,
                "output_data": output_data,
                "expected_count": expected_count,
                "actual_count": result_count,
                "status": "PASS" if test_passed else "FAIL"
            })
            
            logger.info(f"Test Scenario 1 - Insert: {'PASS' if test_passed else 'FAIL'}")
            return test_passed
            
        except Exception as e:
            logger.error(f"Test Scenario 1 failed: {e}")
            self.test_results.append({
                "scenario": "Scenario 1: Insert",
                "input_data": [],
                "output_data": [],
                "expected_count": 1,
                "actual_count": 0,
                "status": "FAIL",
                "error": str(e)
            })
            return False
    
    def test_scenario_2_update(self):
        """Test Scenario 2: Update existing records in target table"""
        logger.info("=== Starting Test Scenario 2: Update ===")
        
        try:
            # Create initial data and write to table
            initial_data = self.create_initial_data()
            target_path = f"{self.temp_dir}/branch_summary_update"
            
            # Create and write initial branch summary
            initial_summary = self.create_branch_summary_report(
                initial_data["transaction"],
                initial_data["account"],
                initial_data["branch"],
                initial_data["branch_operational"]
            )
            initial_summary.write.format("delta").mode("overwrite").save(target_path)
            
            # Create updated data (new transactions for existing branch)
            updated_transaction_schema = StructType([
                StructField("TRANSACTION_ID", IntegerType(), True),
                StructField("ACCOUNT_ID", IntegerType(), True),
                StructField("AMOUNT", DoubleType(), True),
                StructField("TRANSACTION_TYPE", StringType(), True),
                StructField("TRANSACTION_DATE", DateType(), True)
            ])
            updated_transaction_data = [
                (1001, 101, 1000.0, "DEPOSIT", date(2024, 1, 15)),
                (1002, 102, 500.0, "WITHDRAWAL", date(2024, 1, 16)),
                (1003, 101, 2000.0, "DEPOSIT", date(2024, 1, 17))  # New transaction
            ]
            updated_transaction_df = self.spark.createDataFrame(updated_transaction_data, updated_transaction_schema)
            
            # Updated branch operational details
            updated_branch_operational_schema = StructType([
                StructField("BRANCH_ID", IntegerType(), True),
                StructField("REGION", StringType(), True),
                StructField("MANAGER_NAME", StringType(), True),
                StructField("LAST_AUDIT_DATE", DateType(), True),
                StructField("IS_ACTIVE", StringType(), True)
            ])
            updated_branch_operational_data = [(1, "North Region Updated", "Alice Manager", date(2024, 1, 20), "Y")]
            updated_branch_operational_df = self.spark.createDataFrame(updated_branch_operational_data, updated_branch_operational_schema)
            
            # Create updated branch summary
            updated_summary = self.create_branch_summary_report(
                updated_transaction_df,
                initial_data["account"],
                initial_data["branch"],
                updated_branch_operational_df
            )
            
            # Write updated data (simulating update)
            updated_summary.write.format("delta").mode("overwrite").save(target_path)
            
            # Read back and validate
            result_df = self.spark.read.format("delta").load(target_path)
            result_data = result_df.collect()
            
            # Validation - check if total amount and transactions are updated
            expected_total_transactions = 3
            expected_total_amount = 3500.0  # 1000 + 500 + 2000
            
            actual_total_transactions = result_data[0]['TOTAL_TRANSACTIONS']
            actual_total_amount = result_data[0]['TOTAL_AMOUNT']
            
            test_passed = (actual_total_transactions == expected_total_transactions and 
                          actual_total_amount == expected_total_amount)
            
            self.test_results.append({
                "scenario": "Scenario 2: Update",
                "input_data": updated_transaction_data,
                "output_data": result_data,
                "expected_transactions": expected_total_transactions,
                "actual_transactions": actual_total_transactions,
                "expected_amount": expected_total_amount,
                "actual_amount": actual_total_amount,
                "status": "PASS" if test_passed else "FAIL"
            })
            
            logger.info(f"Test Scenario 2 - Update: {'PASS' if test_passed else 'FAIL'}")
            return test_passed
            
        except Exception as e:
            logger.error(f"Test Scenario 2 failed: {e}")
            self.test_results.append({
                "scenario": "Scenario 2: Update",
                "input_data": [],
                "output_data": [],
                "expected_transactions": 3,
                "actual_transactions": 0,
                "expected_amount": 3500.0,
                "actual_amount": 0.0,
                "status": "FAIL",
                "error": str(e)
            })
            return False
    
    def generate_test_report(self):
        """Generate markdown test report"""
        report = "# Test Report\n\n"
        
        for result in self.test_results:
            report += f"## {result['scenario']}\n\n"
            
            if result['scenario'] == "Scenario 1: Insert":
                report += "### Input (Transaction Data):\n"
                report += "| TRANSACTION_ID | ACCOUNT_ID | AMOUNT | TRANSACTION_TYPE | TRANSACTION_DATE |\n"
                report += "|----------------|------------|--------|------------------|------------------|\n"
                
                if 'error' not in result:
                    for row in result['input_data']:
                        report += f"| {row['TRANSACTION_ID']} | {row['ACCOUNT_ID']} | {row['AMOUNT']} | {row['TRANSACTION_TYPE']} | {row['TRANSACTION_DATE']} |\n"
                
                report += "\n### Output (Branch Summary):\n"
                report += "| BRANCH_ID | BRANCH_NAME | TOTAL_TRANSACTIONS | TOTAL_AMOUNT | REGION | LAST_AUDIT_DATE |\n"
                report += "|-----------|-------------|-------------------|--------------|--------|-----------------|\n"
                
                if 'error' not in result:
                    for row in result['output_data']:
                        region = row['REGION'] if row['REGION'] else 'NULL'
                        audit_date = row['LAST_AUDIT_DATE'] if row['LAST_AUDIT_DATE'] else 'NULL'
                        report += f"| {row['BRANCH_ID']} | {row['BRANCH_NAME']} | {row['TOTAL_TRANSACTIONS']} | {row['TOTAL_AMOUNT']} | {region} | {audit_date} |\n"
            
            elif result['scenario'] == "Scenario 2: Update":
                report += "### Input (Updated Transaction Data):\n"
                report += "| TRANSACTION_ID | ACCOUNT_ID | AMOUNT | TRANSACTION_TYPE | TRANSACTION_DATE |\n"
                report += "|----------------|------------|--------|------------------|------------------|\n"
                
                if 'error' not in result:
                    for row in result['input_data']:
                        if isinstance(row, tuple):
                            report += f"| {row[0]} | {row[1]} | {row[2]} | {row[3]} | {row[4]} |\n"
                
                report += "\n### Output (Updated Branch Summary):\n"
                report += "| BRANCH_ID | BRANCH_NAME | TOTAL_TRANSACTIONS | TOTAL_AMOUNT | REGION | LAST_AUDIT_DATE |\n"
                report += "|-----------|-------------|-------------------|--------------|--------|-----------------|\n"
                
                if 'error' not in result:
                    for row in result['output_data']:
                        region = row['REGION'] if row['REGION'] else 'NULL'
                        audit_date = row['LAST_AUDIT_DATE'] if row['LAST_AUDIT_DATE'] else 'NULL'
                        report += f"| {row['BRANCH_ID']} | {row['BRANCH_NAME']} | {row['TOTAL_TRANSACTIONS']} | {row['TOTAL_AMOUNT']} | {region} | {audit_date} |\n"
            
            if 'error' in result:
                report += f"### Error: {result['error']}\n\n"
            
            report += f"### Status: **{result['status']}**\n\n"
            report += "---\n\n"
        
        return report
    
    def run_all_tests(self):
        """Run all test scenarios"""
        logger.info("Starting RegulatoryReportingETL Test Suite")
        
        if not self.setup():
            return False
        
        try:
            # Run test scenarios
            test1_result = self.test_scenario_1_insert()
            test2_result = self.test_scenario_2_update()
            
            # Generate and print report
            report = self.generate_test_report()
            print("\n" + "="*80)
            print("TEST EXECUTION REPORT")
            print("="*80)
            print(report)
            
            # Summary
            total_tests = len(self.test_results)
            passed_tests = sum(1 for result in self.test_results if result['status'] == 'PASS')
            
            print(f"\n## Test Summary")
            print(f"Total Tests: {total_tests}")
            print(f"Passed: {passed_tests}")
            print(f"Failed: {total_tests - passed_tests}")
            print(f"Success Rate: {(passed_tests/total_tests)*100:.1f}%")
            
            return test1_result and test2_result
            
        finally:
            self.teardown()

def main():
    """Main test execution function"""
    test_suite = RegulatoryReportingETLTest()
    success = test_suite.run_all_tests()
    
    if success:
        logger.info("All tests passed successfully!")
    else:
        logger.error("Some tests failed. Check the report above.")
    
    return success

if __name__ == "__main__":
    main()
