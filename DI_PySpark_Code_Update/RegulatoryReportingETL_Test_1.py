# _____________________________________________
# ## *Author*: AAVA
# ## *Created on*:   
# ## *Description*: Python-based test script for RegulatoryReportingETL Pipeline with BRANCH_OPERATIONAL_DETAILS integration
# ## *Version*: 1 
# ## *Updated on*: 
# _____________________________________________

import logging
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, count, sum as spark_sum, when, lit
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DecimalType, DateType, LongType, DoubleType
import sys
from datetime import datetime

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class TestRegulatoryReportingETL:
    """
    Test class for RegulatoryReportingETL Pipeline.
    Tests both insert and update scenarios without using PyTest framework.
    """
    
    def __init__(self):
        self.spark = self._get_spark_session()
        self.test_results = []
    
    def _get_spark_session(self) -> SparkSession:
        """
        Initialize Spark session for testing.
        """
        try:
            spark = SparkSession.getActiveSession()
            if spark is None:
                spark = SparkSession.builder \
                    .appName("RegulatoryReportingETL_Test") \
                    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
                    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
                    .getOrCreate()
            return spark
        except Exception as e:
            logger.error(f"Error creating Spark session: {e}")
            raise
    
    def create_test_data_scenario1(self) -> tuple:
        """
        Create test data for Scenario 1: Insert new records into target table.
        """
        logger.info("Creating test data for Scenario 1: Insert")
        
        # Customer data
        customer_schema = StructType([
            StructField("CUSTOMER_ID", IntegerType(), True),
            StructField("NAME", StringType(), True),
            StructField("EMAIL", StringType(), True),
            StructField("PHONE", StringType(), True),
            StructField("ADDRESS", StringType(), True),
            StructField("CREATED_DATE", DateType(), True)
        ])
        
        customer_data = [
            (1, "Alice Johnson", "alice.johnson@email.com", "111-222-3333", "100 First St", "2023-01-10"),
            (2, "Bob Smith", "bob.smith@email.com", "444-555-6666", "200 Second Ave", "2023-02-15")
        ]
        customer_df = self.spark.createDataFrame(customer_data, customer_schema)
        
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
            (201, "North Branch", "NB201", "Boston", "MA", "USA"),
            (202, "South Branch", "SB202", "Miami", "FL", "USA")
        ]
        branch_df = self.spark.createDataFrame(branch_data, branch_schema)
        
        # Account data
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
            (2001, 1, 201, "ACC2001", "SAVINGS", 10000.00, "2023-01-15"),
            (2002, 2, 202, "ACC2002", "CHECKING", 5000.00, "2023-02-20")
        ]
        account_df = self.spark.createDataFrame(account_data, account_schema)
        
        # Transaction data
        transaction_schema = StructType([
            StructField("TRANSACTION_ID", IntegerType(), True),
            StructField("ACCOUNT_ID", IntegerType(), True),
            StructField("TRANSACTION_TYPE", StringType(), True),
            StructField("AMOUNT", DecimalType(15,2), True),
            StructField("TRANSACTION_DATE", DateType(), True),
            StructField("DESCRIPTION", StringType(), True)
        ])
        
        transaction_data = [
            (20001, 2001, "DEPOSIT", 2000.00, "2023-05-01", "Initial deposit"),
            (20002, 2001, "WITHDRAWAL", 500.00, "2023-05-02", "ATM withdrawal"),
            (20003, 2002, "DEPOSIT", 1000.00, "2023-05-03", "Payroll deposit"),
            (20004, 2002, "TRANSFER", 300.00, "2023-05-04", "Online transfer")
        ]
        transaction_df = self.spark.createDataFrame(transaction_data, transaction_schema)
        
        # Branch Operational Details data - NEW ACTIVE BRANCHES
        branch_operational_schema = StructType([
            StructField("BRANCH_ID", IntegerType(), True),
            StructField("REGION", StringType(), True),
            StructField("MANAGER_NAME", StringType(), True),
            StructField("LAST_AUDIT_DATE", DateType(), True),
            StructField("IS_ACTIVE", StringType(), True)
        ])
        
        branch_operational_data = [
            (201, "Northeast Region", "John Manager", "2023-04-01", "Y"),
            (202, "Southeast Region", "Jane Manager", "2023-03-15", "Y")
        ]
        branch_operational_df = self.spark.createDataFrame(branch_operational_data, branch_operational_schema)
        
        return customer_df, account_df, transaction_df, branch_df, branch_operational_df
    
    def create_test_data_scenario2(self) -> tuple:
        """
        Create test data for Scenario 2: Update existing records in target table.
        """
        logger.info("Creating test data for Scenario 2: Update")
        
        # Reuse same customer and branch structure but with updated operational details
        customer_df, account_df, transaction_df, branch_df, _ = self.create_test_data_scenario1()
        
        # Additional transactions for existing branches
        transaction_schema = StructType([
            StructField("TRANSACTION_ID", IntegerType(), True),
            StructField("ACCOUNT_ID", IntegerType(), True),
            StructField("TRANSACTION_TYPE", StringType(), True),
            StructField("AMOUNT", DecimalType(15,2), True),
            StructField("TRANSACTION_DATE", DateType(), True),
            StructField("DESCRIPTION", StringType(), True)
        ])
        
        additional_transaction_data = [
            (20005, 2001, "DEPOSIT", 1500.00, "2023-05-05", "Bonus deposit"),
            (20006, 2002, "WITHDRAWAL", 200.00, "2023-05-06", "Cash withdrawal")
        ]
        additional_transaction_df = self.spark.createDataFrame(additional_transaction_data, transaction_schema)
        
        # Union with existing transactions to simulate updates
        updated_transaction_df = transaction_df.union(additional_transaction_df)
        
        # Updated Branch Operational Details - SAME BRANCHES WITH UPDATED AUDIT DATES
        branch_operational_schema = StructType([
            StructField("BRANCH_ID", IntegerType(), True),
            StructField("REGION", StringType(), True),
            StructField("MANAGER_NAME", StringType(), True),
            StructField("LAST_AUDIT_DATE", DateType(), True),
            StructField("IS_ACTIVE", StringType(), True)
        ])
        
        updated_branch_operational_data = [
            (201, "Northeast Region", "John Manager", "2023-05-01", "Y"),  # Updated audit date
            (202, "Southeast Region", "Jane Manager", "2023-04-20", "N")   # Changed to inactive
        ]
        updated_branch_operational_df = self.spark.createDataFrame(updated_branch_operational_data, branch_operational_schema)
        
        return customer_df, account_df, updated_transaction_df, branch_df, updated_branch_operational_df
    
    def create_enhanced_branch_summary_report(self, transaction_df: DataFrame, account_df: DataFrame, 
                                            branch_df: DataFrame, branch_operational_df: DataFrame) -> DataFrame:
        """
        Replicated logic from main pipeline for testing.
        """
        # Step 1: Create base branch summary with transaction aggregations
        base_summary = transaction_df.join(account_df, "ACCOUNT_ID") \
                                     .join(branch_df, "BRANCH_ID") \
                                     .groupBy("BRANCH_ID", "BRANCH_NAME") \
                                     .agg(
                                         count("*").alias("TOTAL_TRANSACTIONS"),
                                         spark_sum("AMOUNT").alias("TOTAL_AMOUNT")
                                     )
        
        # Step 2: Join with BRANCH_OPERATIONAL_DETAILS and apply conditional logic
        enhanced_summary = base_summary.join(
            branch_operational_df, 
            base_summary["BRANCH_ID"] == branch_operational_df["BRANCH_ID"], 
            "left"
        ).select(
            base_summary["BRANCH_ID"].cast(LongType()).alias("BRANCH_ID"),
            base_summary["BRANCH_NAME"],
            base_summary["TOTAL_TRANSACTIONS"],
            base_summary["TOTAL_AMOUNT"].cast(DoubleType()).alias("TOTAL_AMOUNT"),
            when(branch_operational_df["IS_ACTIVE"] == "Y", branch_operational_df["REGION"]).alias("REGION"),
            when(branch_operational_df["IS_ACTIVE"] == "Y", branch_operational_df["LAST_AUDIT_DATE"].cast(StringType())).alias("LAST_AUDIT_DATE")
        )
        
        return enhanced_summary
    
    def test_scenario_1_insert(self):
        """
        Test Scenario 1: Insert new records into target table.
        """
        logger.info("\n=== Testing Scenario 1: Insert ===")
        
        try:
            # Create test data
            customer_df, account_df, transaction_df, branch_df, branch_operational_df = self.create_test_data_scenario1()
            
            # Execute the enhanced branch summary logic
            result_df = self.create_enhanced_branch_summary_report(
                transaction_df, account_df, branch_df, branch_operational_df
            )
            
            # Collect results for validation
            results = result_df.collect()
            
            # Validation: Check if records were inserted
            expected_branches = {201, 202}
            actual_branches = {row['BRANCH_ID'] for row in results}
            
            # Validation: Check if REGION and LAST_AUDIT_DATE are populated for active branches
            active_branches_with_region = [row for row in results if row['REGION'] is not None]
            
            test_passed = (
                len(results) == 2 and  # Should have 2 branches
                expected_branches == actual_branches and  # Should have correct branch IDs
                len(active_branches_with_region) == 2 and  # Both branches should have region (both are active)
                all(row['TOTAL_TRANSACTIONS'] > 0 for row in results)  # All should have transactions
            )
            
            self.test_results.append({
                'scenario': 'Scenario 1: Insert',
                'status': 'PASS' if test_passed else 'FAIL',
                'input_data': {
                    'branches': 2,
                    'transactions': transaction_df.count(),
                    'active_operational_details': 2
                },
                'output_data': results,
                'validation': {
                    'expected_branches': len(expected_branches),
                    'actual_branches': len(actual_branches),
                    'branches_with_region': len(active_branches_with_region)
                }
            })
            
            logger.info(f"Scenario 1 Test Result: {'PASS' if test_passed else 'FAIL'}")
            return test_passed
            
        except Exception as e:
            logger.error(f"Scenario 1 test failed with exception: {e}")
            self.test_results.append({
                'scenario': 'Scenario 1: Insert',
                'status': 'FAIL',
                'error': str(e)
            })
            return False
    
    def test_scenario_2_update(self):
        """
        Test Scenario 2: Update existing records in target table.
        """
        logger.info("\n=== Testing Scenario 2: Update ===")
        
        try:
            # Create test data with updates
            customer_df, account_df, transaction_df, branch_df, branch_operational_df = self.create_test_data_scenario2()
            
            # Execute the enhanced branch summary logic
            result_df = self.create_enhanced_branch_summary_report(
                transaction_df, account_df, branch_df, branch_operational_df
            )
            
            # Collect results for validation
            results = result_df.collect()
            
            # Validation: Check if updates were applied correctly
            branch_201_result = next((row for row in results if row['BRANCH_ID'] == 201), None)
            branch_202_result = next((row for row in results if row['BRANCH_ID'] == 202), None)
            
            test_passed = (
                len(results) == 2 and  # Should still have 2 branches
                branch_201_result is not None and
                branch_202_result is not None and
                branch_201_result['REGION'] is not None and  # Branch 201 is active, should have region
                branch_202_result['REGION'] is None and     # Branch 202 is inactive, should not have region
                branch_201_result['TOTAL_TRANSACTIONS'] == 3 and  # Updated transaction count
                branch_202_result['TOTAL_TRANSACTIONS'] == 3      # Updated transaction count
            )
            
            self.test_results.append({
                'scenario': 'Scenario 2: Update',
                'status': 'PASS' if test_passed else 'FAIL',
                'input_data': {
                    'branches': 2,
                    'transactions': transaction_df.count(),
                    'active_operational_details': 1,  # Only branch 201 is active
                    'inactive_operational_details': 1  # Branch 202 is inactive
                },
                'output_data': results,
                'validation': {
                    'branch_201_active': branch_201_result['REGION'] is not None if branch_201_result else False,
                    'branch_202_inactive': branch_202_result['REGION'] is None if branch_202_result else False
                }
            })
            
            logger.info(f"Scenario 2 Test Result: {'PASS' if test_passed else 'FAIL'}")
            return test_passed
            
        except Exception as e:
            logger.error(f"Scenario 2 test failed with exception: {e}")
            self.test_results.append({
                'scenario': 'Scenario 2: Update',
                'status': 'FAIL',
                'error': str(e)
            })
            return False
    
    def generate_markdown_report(self) -> str:
        """
        Generate a markdown report of test results.
        """
        report = "# Test Report\n\n"
        report += f"**Test Execution Time:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n"
        
        for result in self.test_results:
            report += f"## {result['scenario']}\n\n"
            
            if 'error' in result:
                report += f"**Status:** {result['status']}\n"
                report += f"**Error:** {result['error']}\n\n"
                continue
            
            # Input section
            report += "### Input:\n"
            input_data = result['input_data']
            report += f"- Branches: {input_data['branches']}\n"
            report += f"- Transactions: {input_data['transactions']}\n"
            report += f"- Active Operational Details: {input_data.get('active_operational_details', 'N/A')}\n"
            if 'inactive_operational_details' in input_data:
                report += f"- Inactive Operational Details: {input_data['inactive_operational_details']}\n"
            report += "\n"
            
            # Output section
            report += "### Output:\n"
            report += "| BRANCH_ID | BRANCH_NAME | TOTAL_TRANSACTIONS | TOTAL_AMOUNT | REGION | LAST_AUDIT_DATE |\n"
            report += "|-----------|-------------|-------------------|--------------|--------|----------------|\n"
            
            for row in result['output_data']:
                region = row['REGION'] if row['REGION'] else 'NULL'
                audit_date = row['LAST_AUDIT_DATE'] if row['LAST_AUDIT_DATE'] else 'NULL'
                report += f"| {row['BRANCH_ID']} | {row['BRANCH_NAME']} | {row['TOTAL_TRANSACTIONS']} | {row['TOTAL_AMOUNT']:.2f} | {region} | {audit_date} |\n"
            
            report += "\n"
            
            # Status
            report += f"**Status:** {result['status']}\n\n"
            
            # Validation details
            if 'validation' in result:
                report += "### Validation Details:\n"
                validation = result['validation']
                for key, value in validation.items():
                    report += f"- {key.replace('_', ' ').title()}: {value}\n"
                report += "\n"
        
        return report
    
    def run_all_tests(self):
        """
        Run all test scenarios and generate report.
        """
        logger.info("Starting RegulatoryReportingETL Test Suite")
        
        # Run tests
        scenario1_passed = self.test_scenario_1_insert()
        scenario2_passed = self.test_scenario_2_update()
        
        # Generate and print report
        report = self.generate_markdown_report()
        print("\n" + "="*80)
        print("TEST EXECUTION REPORT")
        print("="*80)
        print(report)
        
        # Summary
        total_tests = len(self.test_results)
        passed_tests = sum(1 for result in self.test_results if result['status'] == 'PASS')
        
        logger.info(f"Test Summary: {passed_tests}/{total_tests} tests passed")
        
        return scenario1_passed and scenario2_passed

def main():
    """
    Main test execution function.
    """
    try:
        test_suite = TestRegulatoryReportingETL()
        all_tests_passed = test_suite.run_all_tests()
        
        if all_tests_passed:
            logger.info("All tests passed successfully!")
            return 0
        else:
            logger.error("Some tests failed!")
            return 1
            
    except Exception as e:
        logger.error(f"Test execution failed: {e}")
        return 1

if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)