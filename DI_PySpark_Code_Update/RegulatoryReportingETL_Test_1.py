_____________________________________________
## *Author*: AAVA
## *Created on*:   
## *Description*: Python test script for RegulatoryReportingETL pipeline validation
## *Version*: 1 
## *Updated on*: 
_____________________________________________

import logging
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, count, sum as spark_sum, when, lit
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DecimalType, DateType, LongType, DoubleType
from datetime import date
import sys

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class ETLTester:
    def __init__(self):
        self.spark = self._get_spark_session()
        self.test_results = []
        
    def _get_spark_session(self) -> SparkSession:
        """Initialize Spark session for testing"""
        try:
            spark = SparkSession.getActiveSession()
            if spark is None:
                spark = SparkSession.builder \
                    .appName("ETL_Testing") \
                    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
                    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
                    .getOrCreate()
            return spark
        except Exception as e:
            logger.error(f"Error creating Spark session: {e}")
            raise
    
    def create_test_data_scenario1(self) -> tuple:
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
        
        branch_df = self.spark.createDataFrame(branch_data, branch_schema)
        
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
            (2001, 10, 201, "ACC2001", "SAVINGS", 3000.00, date(2023, 6, 1)),
            (2002, 11, 202, "ACC2002", "CHECKING", 1500.00, date(2023, 6, 2))
        ]
        
        account_df = self.spark.createDataFrame(account_data, account_schema)
        
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
            (20001, 2001, "DEPOSIT", 500.00, date(2023, 6, 5), "Initial deposit"),
            (20002, 2001, "WITHDRAWAL", 100.00, date(2023, 6, 6), "ATM withdrawal"),
            (20003, 2002, "DEPOSIT", 750.00, date(2023, 6, 7), "Salary deposit")
        ]
        
        transaction_df = self.spark.createDataFrame(transaction_data, transaction_schema)
        
        # Branch Operational Details
        branch_operational_schema = StructType([
            StructField("BRANCH_ID", IntegerType(), True),
            StructField("REGION", StringType(), True),
            StructField("MANAGER_NAME", StringType(), True),
            StructField("LAST_AUDIT_DATE", DateType(), True),
            StructField("IS_ACTIVE", StringType(), True)
        ])
        
        branch_operational_data = [
            (201, "Northeast Region", "David Manager", date(2023, 5, 15), "Y"),
            (202, "Northwest Region", "Eva Manager", date(2023, 5, 20), "Y")
        ]
        
        branch_operational_df = self.spark.createDataFrame(branch_operational_data, branch_operational_schema)
        
        return transaction_df, account_df, branch_df, branch_operational_df
    
    def create_test_data_scenario2(self) -> tuple:
        """Create test data for Scenario 2: Update existing records"""
        logger.info("Creating test data for Scenario 2: Update")
        
        # Branch data (existing branches)
        branch_schema = StructType([
            StructField("BRANCH_ID", IntegerType(), True),
            StructField("BRANCH_NAME", StringType(), True),
            StructField("BRANCH_CODE", StringType(), True),
            StructField("CITY", StringType(), True),
            StructField("STATE", StringType(), True),
            StructField("COUNTRY", StringType(), True)
        ])
        
        branch_data = [
            (101, "Downtown Branch Updated", "DT001", "New York", "NY", "USA"),
            (102, "Uptown Branch Updated", "UT002", "Los Angeles", "CA", "USA")
        ]
        
        branch_df = self.spark.createDataFrame(branch_data, branch_schema)
        
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
            (3001, 1, 101, "ACC3001", "SAVINGS", 8000.00, date(2023, 7, 1)),
            (3002, 2, 102, "ACC3002", "CHECKING", 4500.00, date(2023, 7, 2))
        ]
        
        account_df = self.spark.createDataFrame(account_data, account_schema)
        
        # Transaction data (additional transactions for existing branches)
        transaction_schema = StructType([
            StructField("TRANSACTION_ID", IntegerType(), True),
            StructField("ACCOUNT_ID", IntegerType(), True),
            StructField("TRANSACTION_TYPE", StringType(), True),
            StructField("AMOUNT", DecimalType(15, 2), True),
            StructField("TRANSACTION_DATE", DateType(), True),
            StructField("DESCRIPTION", StringType(), True)
        ])
        
        transaction_data = [
            (30001, 3001, "DEPOSIT", 2000.00, date(2023, 7, 5), "Large deposit"),
            (30002, 3001, "TRANSFER", 500.00, date(2023, 7, 6), "Internal transfer"),
            (30003, 3002, "DEPOSIT", 1200.00, date(2023, 7, 7), "Business deposit"),
            (30004, 3002, "WITHDRAWAL", 300.00, date(2023, 7, 8), "Cash withdrawal")
        ]
        
        transaction_df = self.spark.createDataFrame(transaction_data, transaction_schema)
        
        # Branch Operational Details (updated information)
        branch_operational_schema = StructType([
            StructField("BRANCH_ID", IntegerType(), True),
            StructField("REGION", StringType(), True),
            StructField("MANAGER_NAME", StringType(), True),
            StructField("LAST_AUDIT_DATE", DateType(), True),
            StructField("IS_ACTIVE", StringType(), True)
        ])
        
        branch_operational_data = [
            (101, "East Region Updated", "Alice Manager Updated", date(2023, 7, 15), "Y"),
            (102, "West Region Updated", "Bob Manager Updated", date(2023, 7, 20), "Y")
        ]
        
        branch_operational_df = self.spark.createDataFrame(branch_operational_data, branch_operational_schema)
        
        return transaction_df, account_df, branch_df, branch_operational_df
    
    def create_enhanced_branch_summary_report(self, transaction_df: DataFrame, account_df: DataFrame, 
                                            branch_df: DataFrame, branch_operational_df: DataFrame) -> DataFrame:
        """Create enhanced branch summary report (same logic as main pipeline)"""
        
        # Step 1: Create base branch summary with transaction aggregations
        base_summary = transaction_df.join(account_df, "ACCOUNT_ID") \
                                     .join(branch_df, "BRANCH_ID") \
                                     .groupBy("BRANCH_ID", "BRANCH_NAME") \
                                     .agg(
                                         count("*").alias("TOTAL_TRANSACTIONS"),
                                         spark_sum("AMOUNT").alias("TOTAL_AMOUNT")
                                     )
        
        # Step 2: Join with BRANCH_OPERATIONAL_DETAILS and apply conditional logic
        enhanced_summary = base_summary.join(branch_operational_df, "BRANCH_ID", "left") \
                                       .select(
                                           col("BRANCH_ID").cast(LongType()),
                                           col("BRANCH_NAME"),
                                           col("TOTAL_TRANSACTIONS"),
                                           col("TOTAL_AMOUNT").cast(DoubleType()),
                                           when(col("IS_ACTIVE") == "Y", col("REGION")).otherwise(lit(None)).alias("REGION"),
                                           when(col("IS_ACTIVE") == "Y", col("LAST_AUDIT_DATE").cast(StringType())).otherwise(lit(None)).alias("LAST_AUDIT_DATE")
                                       )
        
        return enhanced_summary
    
    def test_scenario_1_insert(self):
        """Test Scenario 1: Insert new records into target table"""
        logger.info("\n=== Testing Scenario 1: Insert ===")
        
        try:
            # Create test data
            transaction_df, account_df, branch_df, branch_operational_df = self.create_test_data_scenario1()
            
            # Execute ETL logic
            result_df = self.create_enhanced_branch_summary_report(
                transaction_df, account_df, branch_df, branch_operational_df
            )
            
            # Collect results
            results = result_df.collect()
            
            # Validate results
            expected_branches = {201, 202}
            actual_branches = {row['BRANCH_ID'] for row in results}
            
            # Check if all expected branches are present
            if expected_branches == actual_branches:
                # Check if REGION and LAST_AUDIT_DATE are populated (since IS_ACTIVE = 'Y')
                all_regions_populated = all(row['REGION'] is not None for row in results)
                all_audit_dates_populated = all(row['LAST_AUDIT_DATE'] is not None for row in results)
                
                if all_regions_populated and all_audit_dates_populated:
                    status = "PASS"
                    message = "All new records inserted successfully with proper REGION and LAST_AUDIT_DATE population"
                else:
                    status = "FAIL"
                    message = "Records inserted but REGION or LAST_AUDIT_DATE not properly populated"
            else:
                status = "FAIL"
                message = f"Expected branches {expected_branches}, but got {actual_branches}"
            
            self.test_results.append({
                'scenario': 'Scenario 1: Insert',
                'input_data': 'New branches 201, 202 with transactions',
                'output_data': results,
                'status': status,
                'message': message
            })
            
            logger.info(f"Scenario 1 Status: {status} - {message}")
            
        except Exception as e:
            logger.error(f"Scenario 1 failed with error: {e}")
            self.test_results.append({
                'scenario': 'Scenario 1: Insert',
                'input_data': 'New branches 201, 202 with transactions',
                'output_data': [],
                'status': 'FAIL',
                'message': f"Exception occurred: {str(e)}"
            })
    
    def test_scenario_2_update(self):
        """Test Scenario 2: Update existing records in target table"""
        logger.info("\n=== Testing Scenario 2: Update ===")
        
        try:
            # Create test data for updates
            transaction_df, account_df, branch_df, branch_operational_df = self.create_test_data_scenario2()
            
            # Execute ETL logic
            result_df = self.create_enhanced_branch_summary_report(
                transaction_df, account_df, branch_df, branch_operational_df
            )
            
            # Collect results
            results = result_df.collect()
            
            # Validate results
            expected_branches = {101, 102}
            actual_branches = {row['BRANCH_ID'] for row in results}
            
            # Check if expected branches are present
            if expected_branches == actual_branches:
                # Check if branch names are updated
                branch_names = {row['BRANCH_NAME'] for row in results}
                expected_updated_names = {"Downtown Branch Updated", "Uptown Branch Updated"}
                
                if expected_updated_names.issubset(branch_names):
                    # Check if REGION and LAST_AUDIT_DATE are updated
                    regions = {row['REGION'] for row in results if row['REGION'] is not None}
                    expected_regions = {"East Region Updated", "West Region Updated"}
                    
                    if expected_regions == regions:
                        status = "PASS"
                        message = "Existing records updated successfully with new REGION and LAST_AUDIT_DATE values"
                    else:
                        status = "FAIL"
                        message = f"Expected regions {expected_regions}, but got {regions}"
                else:
                    status = "FAIL"
                    message = "Branch names not properly updated"
            else:
                status = "FAIL"
                message = f"Expected branches {expected_branches}, but got {actual_branches}"
            
            self.test_results.append({
                'scenario': 'Scenario 2: Update',
                'input_data': 'Updated data for existing branches 101, 102',
                'output_data': results,
                'status': status,
                'message': message
            })
            
            logger.info(f"Scenario 2 Status: {status} - {message}")
            
        except Exception as e:
            logger.error(f"Scenario 2 failed with error: {e}")
            self.test_results.append({
                'scenario': 'Scenario 2: Update',
                'input_data': 'Updated data for existing branches 101, 102',
                'output_data': [],
                'status': 'FAIL',
                'message': f"Exception occurred: {str(e)}"
            })
    
    def generate_markdown_report(self):
        """Generate markdown test report"""
        logger.info("\n=== Generating Test Report ===")
        
        report = "# Test Report\n\n"
        
        for result in self.test_results:
            report += f"## {result['scenario']}\n\n"
            report += f"**Input:** {result['input_data']}\n\n"
            
            if result['output_data']:
                report += "**Output:**\n\n"
                report += "| BRANCH_ID | BRANCH_NAME | TOTAL_TRANSACTIONS | TOTAL_AMOUNT | REGION | LAST_AUDIT_DATE |\n"
                report += "|-----------|-------------|-------------------|--------------|--------|-----------------|\n"
                
                for row in result['output_data']:
                    region = row['REGION'] if row['REGION'] else 'NULL'
                    audit_date = row['LAST_AUDIT_DATE'] if row['LAST_AUDIT_DATE'] else 'NULL'
                    report += f"| {row['BRANCH_ID']} | {row['BRANCH_NAME']} | {row['TOTAL_TRANSACTIONS']} | {row['TOTAL_AMOUNT']:.2f} | {region} | {audit_date} |\n"
            else:
                report += "**Output:** No data generated\n\n"
            
            report += f"\n**Status:** {result['status']}\n\n"
            report += f"**Message:** {result['message']}\n\n"
            report += "---\n\n"
        
        return report
    
    def run_all_tests(self):
        """Run all test scenarios"""
        logger.info("Starting ETL Pipeline Tests")
        
        # Run test scenarios
        self.test_scenario_1_insert()
        self.test_scenario_2_update()
        
        # Generate and display report
        report = self.generate_markdown_report()
        print(report)
        
        # Summary
        passed_tests = sum(1 for result in self.test_results if result['status'] == 'PASS')
        total_tests = len(self.test_results)
        
        logger.info(f"\n=== Test Summary ===")
        logger.info(f"Total Tests: {total_tests}")
        logger.info(f"Passed: {passed_tests}")
        logger.info(f"Failed: {total_tests - passed_tests}")
        
        if passed_tests == total_tests:
            logger.info("All tests PASSED! ✅")
        else:
            logger.warning(f"{total_tests - passed_tests} test(s) FAILED! ❌")
        
        return passed_tests == total_tests

def main():
    """Main test execution function"""
    try:
        tester = ETLTester()
        success = tester.run_all_tests()
        
        if success:
            logger.info("All tests completed successfully")
            return 0
        else:
            logger.error("Some tests failed")
            return 1
            
    except Exception as e:
        logger.error(f"Test execution failed: {e}")
        return 1

if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)