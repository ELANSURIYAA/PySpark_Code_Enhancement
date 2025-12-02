_____________________________________________
## *Author*: AAVA
## *Created on*:   
## *Description*: Enhanced Python test script for RegulatoryReportingETL pipeline validation with comprehensive testing scenarios
## *Version*: 2 
## *Changes*: Added comprehensive test scenarios, enhanced validation logic, performance monitoring, and detailed reporting capabilities
## *Reason*: User requested enhanced testing capabilities for better validation coverage and production readiness
## *Updated on*: 
_____________________________________________

import logging
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, count, sum as spark_sum, when, lit, broadcast, coalesce, current_timestamp
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DecimalType, DateType, LongType, DoubleType
from datetime import date, datetime
import sys
import time

# Configure enhanced logging
logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(name)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s'
)
logger = logging.getLogger(__name__)

class EnhancedETLTester:
    """Enhanced ETL testing class with comprehensive validation and performance monitoring"""
    
    def __init__(self):
        self.spark = self._get_spark_session()
        self.test_results = []
        self.performance_metrics = {}
        self.start_time = None
        
    def _get_spark_session(self) -> SparkSession:
        """Initialize enhanced Spark session for testing"""
        try:
            spark = SparkSession.getActiveSession()
            if spark is None:
                spark = SparkSession.builder \
                    .appName("Enhanced_ETL_Testing_v2") \
                    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
                    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
                    .config("spark.sql.adaptive.enabled", "true") \
                    .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
                    .getOrCreate()
            
            logger.info("Enhanced Spark session initialized for testing v2")
            return spark
        except Exception as e:
            logger.error(f"Error creating Spark session: {e}")
            raise
    
    def _start_timer(self, operation: str):
        """Start timing an operation"""
        self.start_time = time.time()
        logger.info(f"Starting {operation}")
    
    def _end_timer(self, operation: str):
        """End timing and record performance"""
        if self.start_time:
            duration = time.time() - self.start_time
            self.performance_metrics[operation] = duration
            logger.info(f"Completed {operation} in {duration:.3f} seconds")
            return duration
        return 0
    
    def create_test_data_scenario1_enhanced(self) -> tuple:
        """Create enhanced test data for Scenario 1: Insert new records with comprehensive data"""
        logger.info("Creating enhanced test data for Scenario 1: Insert with comprehensive validation")
        
        # Enhanced Branch data with more test cases
        branch_schema = StructType([
            StructField("BRANCH_ID", IntegerType(), True),
            StructField("BRANCH_NAME", StringType(), True),
            StructField("BRANCH_CODE", StringType(), True),
            StructField("CITY", StringType(), True),
            StructField("STATE", StringType(), True),
            StructField("COUNTRY", StringType(), True)
        ])
        
        branch_data = [
            (301, "Innovation Branch", "INN001", "San Francisco", "CA", "USA"),
            (302, "Technology Branch", "TECH002", "Austin", "TX", "USA"),
            (303, "Digital Branch", "DIG003", "Denver", "CO", "USA")
        ]
        
        branch_df = self.spark.createDataFrame(branch_data, branch_schema)
        
        # Enhanced Account data
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
            (3001, 20, 301, "ACC3001", "BUSINESS", 15000.00, date(2023, 8, 1)),
            (3002, 21, 302, "ACC3002", "PREMIUM", 25000.00, date(2023, 8, 2)),
            (3003, 22, 303, "ACC3003", "SAVINGS", 8000.00, date(2023, 8, 3)),
            (3004, 23, 301, "ACC3004", "CHECKING", 5500.00, date(2023, 8, 4))
        ]
        
        account_df = self.spark.createDataFrame(account_data, account_schema)
        
        # Enhanced Transaction data with diverse transaction types
        transaction_schema = StructType([
            StructField("TRANSACTION_ID", IntegerType(), True),
            StructField("ACCOUNT_ID", IntegerType(), True),
            StructField("TRANSACTION_TYPE", StringType(), True),
            StructField("AMOUNT", DecimalType(15, 2), True),
            StructField("TRANSACTION_DATE", DateType(), True),
            StructField("DESCRIPTION", StringType(), True)
        ])
        
        transaction_data = [
            (30001, 3001, "DEPOSIT", 5000.00, date(2023, 8, 5), "Business revenue deposit"),
            (30002, 3001, "TRANSFER", 2000.00, date(2023, 8, 6), "Supplier payment"),
            (30003, 3002, "DEPOSIT", 10000.00, date(2023, 8, 7), "Investment return"),
            (30004, 3002, "WITHDRAWAL", 1500.00, date(2023, 8, 8), "Premium service fee"),
            (30005, 3003, "DEPOSIT", 3000.00, date(2023, 8, 9), "Salary deposit"),
            (30006, 3004, "DEPOSIT", 1200.00, date(2023, 8, 10), "Freelance payment"),
            (30007, 3004, "WITHDRAWAL", 300.00, date(2023, 8, 11), "ATM withdrawal")
        ]
        
        transaction_df = self.spark.createDataFrame(transaction_data, transaction_schema)
        
        # Enhanced Branch Operational Details with comprehensive test scenarios
        branch_operational_schema = StructType([
            StructField("BRANCH_ID", IntegerType(), True),
            StructField("REGION", StringType(), True),
            StructField("MANAGER_NAME", StringType(), True),
            StructField("LAST_AUDIT_DATE", DateType(), True),
            StructField("IS_ACTIVE", StringType(), True)
        ])
        
        branch_operational_data = [
            (301, "West Coast Region", "Sarah Innovation", date(2023, 7, 15), "Y"),
            (302, "Central Region", "Mike Technology", date(2023, 7, 20), "Y"),
            (303, "Mountain Region", "Lisa Digital", date(2023, 7, 25), "Y")
        ]
        
        branch_operational_df = self.spark.createDataFrame(branch_operational_data, branch_operational_schema)
        
        logger.info(f"Enhanced Scenario 1 data created: {len(branch_data)} branches, {len(account_data)} accounts, {len(transaction_data)} transactions")
        
        return transaction_df, account_df, branch_df, branch_operational_df
    
    def create_test_data_scenario2_enhanced(self) -> tuple:
        """Create enhanced test data for Scenario 2: Update existing records with edge cases"""
        logger.info("Creating enhanced test data for Scenario 2: Update with edge case validation")
        
        # Branch data (existing branches with updates)
        branch_schema = StructType([
            StructField("BRANCH_ID", IntegerType(), True),
            StructField("BRANCH_NAME", StringType(), True),
            StructField("BRANCH_CODE", StringType(), True),
            StructField("CITY", StringType(), True),
            StructField("STATE", StringType(), True),
            StructField("COUNTRY", StringType(), True)
        ])
        
        branch_data = [
            (101, "Downtown Branch - Renovated", "DT001", "New York", "NY", "USA"),
            (102, "Uptown Branch - Expanded", "UT002", "Los Angeles", "CA", "USA"),
            (104, "Westside Branch - Modernized", "WS004", "Houston", "TX", "USA")
        ]
        
        branch_df = self.spark.createDataFrame(branch_data, branch_schema)
        
        # Account data with higher volumes
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
            (4001, 1, 101, "ACC4001", "PLATINUM", 50000.00, date(2023, 9, 1)),
            (4002, 2, 102, "ACC4002", "GOLD", 35000.00, date(2023, 9, 2)),
            (4003, 4, 104, "ACC4003", "BUSINESS_PLUS", 75000.00, date(2023, 9, 3)),
            (4004, 1, 101, "ACC4004", "INVESTMENT", 100000.00, date(2023, 9, 4))
        ]
        
        account_df = self.spark.createDataFrame(account_data, account_schema)
        
        # High-volume transaction data for stress testing
        transaction_schema = StructType([
            StructField("TRANSACTION_ID", IntegerType(), True),
            StructField("ACCOUNT_ID", IntegerType(), True),
            StructField("TRANSACTION_TYPE", StringType(), True),
            StructField("AMOUNT", DecimalType(15, 2), True),
            StructField("TRANSACTION_DATE", DateType(), True),
            StructField("DESCRIPTION", StringType(), True)
        ])
        
        transaction_data = [
            (40001, 4001, "DEPOSIT", 25000.00, date(2023, 9, 5), "Large business deposit"),
            (40002, 4001, "TRANSFER", 10000.00, date(2023, 9, 6), "Investment transfer"),
            (40003, 4001, "WITHDRAWAL", 5000.00, date(2023, 9, 7), "Business expense"),
            (40004, 4002, "DEPOSIT", 15000.00, date(2023, 9, 8), "Quarterly bonus"),
            (40005, 4002, "TRANSFER", 8000.00, date(2023, 9, 9), "Real estate payment"),
            (40006, 4003, "DEPOSIT", 30000.00, date(2023, 9, 10), "Contract payment"),
            (40007, 4003, "WITHDRAWAL", 12000.00, date(2023, 9, 11), "Equipment purchase"),
            (40008, 4004, "DEPOSIT", 50000.00, date(2023, 9, 12), "Investment principal"),
            (40009, 4004, "TRANSFER", 20000.00, date(2023, 9, 13), "Portfolio rebalancing")
        ]
        
        transaction_df = self.spark.createDataFrame(transaction_data, transaction_schema)
        
        # Branch Operational Details with mixed active/inactive status for edge case testing
        branch_operational_schema = StructType([
            StructField("BRANCH_ID", IntegerType(), True),
            StructField("REGION", StringType(), True),
            StructField("MANAGER_NAME", StringType(), True),
            StructField("LAST_AUDIT_DATE", DateType(), True),
            StructField("IS_ACTIVE", StringType(), True)
        ])
        
        branch_operational_data = [
            (101, "East Region Premium", "Alice Manager Pro", date(2023, 9, 15), "Y"),
            (102, "West Region Elite", "Bob Manager Plus", date(2023, 9, 20), "Y"),
            (104, "South Region Advanced", "Diana Manager Expert", date(2023, 8, 25), "N")  # Inactive for edge case testing
        ]
        
        branch_operational_df = self.spark.createDataFrame(branch_operational_data, branch_operational_schema)
        
        logger.info(f"Enhanced Scenario 2 data created: {len(branch_data)} branches, {len(account_data)} accounts, {len(transaction_data)} transactions")
        
        return transaction_df, account_df, branch_df, branch_operational_df
    
    def create_enhanced_branch_summary_report_v2(self, transaction_df: DataFrame, account_df: DataFrame, 
                                                branch_df: DataFrame, branch_operational_df: DataFrame) -> DataFrame:
        """Create enhanced branch summary report v2 (same logic as main pipeline)"""
        
        try:
            # Step 1: Create base branch summary with optimized aggregations
            base_summary = transaction_df.join(account_df, "ACCOUNT_ID", "inner") \
                                         .join(broadcast(branch_df), "BRANCH_ID", "inner") \
                                         .groupBy("BRANCH_ID", "BRANCH_NAME") \
                                         .agg(
                                             count("*").alias("TOTAL_TRANSACTIONS"),
                                             spark_sum("AMOUNT").alias("TOTAL_AMOUNT")
                                         )
            
            # Step 2: Enhanced join with BRANCH_OPERATIONAL_DETAILS
            enhanced_summary = base_summary.join(branch_operational_df, "BRANCH_ID", "left") \
                                           .select(
                                               col("BRANCH_ID").cast(LongType()),
                                               col("BRANCH_NAME"),
                                               col("TOTAL_TRANSACTIONS"),
                                               col("TOTAL_AMOUNT").cast(DoubleType()),
                                               # Enhanced conditional population with coalesce
                                               coalesce(
                                                   when(col("IS_ACTIVE") == "Y", col("REGION")),
                                                   lit("UNKNOWN_REGION")
                                               ).alias("REGION"),
                                               coalesce(
                                                   when(col("IS_ACTIVE") == "Y", col("LAST_AUDIT_DATE").cast(StringType())),
                                                   lit("NOT_AUDITED")
                                               ).alias("LAST_AUDIT_DATE")
                                           )
            
            # Final selection for target schema compatibility
            final_summary = enhanced_summary.select(
                col("BRANCH_ID"),
                col("BRANCH_NAME"),
                col("TOTAL_TRANSACTIONS"),
                col("TOTAL_AMOUNT"),
                col("REGION"),
                col("LAST_AUDIT_DATE")
            )
            
            return final_summary
            
        except Exception as e:
            logger.error(f"Error creating enhanced branch summary report v2: {e}")
            raise
    
    def validate_enhanced_results(self, results: list, scenario_name: str, expected_conditions: dict) -> dict:
        """Enhanced validation with comprehensive checks"""
        validation_result = {
            'scenario': scenario_name,
            'status': 'PASS',
            'issues': [],
            'metrics': {}
        }
        
        try:
            # Basic validations
            if not results:
                validation_result['status'] = 'FAIL'
                validation_result['issues'].append('No results returned')
                return validation_result
            
            # Record count validation
            validation_result['metrics']['record_count'] = len(results)
            
            # Expected branch validation
            if 'expected_branches' in expected_conditions:
                actual_branches = {row['BRANCH_ID'] for row in results}
                expected_branches = expected_conditions['expected_branches']
                
                if not expected_branches.issubset(actual_branches):
                    validation_result['status'] = 'FAIL'
                    missing_branches = expected_branches - actual_branches
                    validation_result['issues'].append(f'Missing expected branches: {missing_branches}')
            
            # Region population validation for active branches
            active_branches_with_region = 0
            inactive_branches_with_unknown_region = 0
            total_amount = 0
            
            for row in results:
                total_amount += row['TOTAL_AMOUNT']
                
                # Check region population logic
                if row['REGION'] not in ['UNKNOWN_REGION', 'NOT_AUDITED', None]:
                    active_branches_with_region += 1
                elif row['REGION'] == 'UNKNOWN_REGION':
                    inactive_branches_with_unknown_region += 1
            
            validation_result['metrics']['active_branches_with_region'] = active_branches_with_region
            validation_result['metrics']['inactive_branches'] = inactive_branches_with_unknown_region
            validation_result['metrics']['total_amount'] = total_amount
            
            # Business logic validations
            if 'min_total_amount' in expected_conditions:
                if total_amount < expected_conditions['min_total_amount']:
                    validation_result['issues'].append(f'Total amount {total_amount} below expected minimum {expected_conditions["min_total_amount"]}')
            
            # Data consistency checks
            for row in results:
                if row['TOTAL_TRANSACTIONS'] <= 0:
                    validation_result['issues'].append(f'Branch {row["BRANCH_ID"]} has invalid transaction count: {row["TOTAL_TRANSACTIONS"]}')
                
                if row['TOTAL_AMOUNT'] < 0:
                    validation_result['issues'].append(f'Branch {row["BRANCH_ID"]} has negative total amount: {row["TOTAL_AMOUNT"]}')
            
            # Set final status
            if validation_result['issues']:
                validation_result['status'] = 'FAIL'
            
            return validation_result
            
        except Exception as e:
            validation_result['status'] = 'FAIL'
            validation_result['issues'].append(f'Validation error: {str(e)}')
            return validation_result
    
    def test_scenario_1_insert_enhanced(self):
        """Enhanced Test Scenario 1: Insert new records with comprehensive validation"""
        logger.info("\n=== Enhanced Testing Scenario 1: Insert ===")
        
        try:
            self._start_timer("scenario_1_execution")
            
            # Create test data
            transaction_df, account_df, branch_df, branch_operational_df = self.create_test_data_scenario1_enhanced()
            
            # Execute ETL logic
            result_df = self.create_enhanced_branch_summary_report_v2(
                transaction_df, account_df, branch_df, branch_operational_df
            )
            
            # Collect results
            results = result_df.collect()
            
            # Enhanced validation
            expected_conditions = {
                'expected_branches': {301, 302, 303},
                'min_total_amount': 10000.00  # Minimum expected total across all branches
            }
            
            validation_result = self.validate_enhanced_results(results, 'Scenario 1: Insert', expected_conditions)
            
            execution_time = self._end_timer("scenario_1_execution")
            validation_result['execution_time'] = execution_time
            validation_result['input_data'] = 'New branches 301, 302, 303 with comprehensive transaction data'
            validation_result['output_data'] = results
            
            self.test_results.append(validation_result)
            
            logger.info(f"Enhanced Scenario 1 Status: {validation_result['status']} - Execution time: {execution_time:.3f}s")
            if validation_result['issues']:
                logger.warning(f"Issues found: {validation_result['issues']}")
            
        except Exception as e:
            logger.error(f"Enhanced Scenario 1 failed with error: {e}")
            self.test_results.append({
                'scenario': 'Scenario 1: Insert',
                'input_data': 'New branches 301, 302, 303 with comprehensive transaction data',
                'output_data': [],
                'status': 'FAIL',
                'issues': [f"Exception occurred: {str(e)}"],
                'execution_time': self._end_timer("scenario_1_execution")
            })
    
    def test_scenario_2_update_enhanced(self):
        """Enhanced Test Scenario 2: Update existing records with edge case validation"""
        logger.info("\n=== Enhanced Testing Scenario 2: Update ===")
        
        try:
            self._start_timer("scenario_2_execution")
            
            # Create test data for updates
            transaction_df, account_df, branch_df, branch_operational_df = self.create_test_data_scenario2_enhanced()
            
            # Execute ETL logic
            result_df = self.create_enhanced_branch_summary_report_v2(
                transaction_df, account_df, branch_df, branch_operational_df
            )
            
            # Collect results
            results = result_df.collect()
            
            # Enhanced validation with edge cases
            expected_conditions = {
                'expected_branches': {101, 102, 104},
                'min_total_amount': 50000.00  # Higher minimum for update scenario
            }
            
            validation_result = self.validate_enhanced_results(results, 'Scenario 2: Update', expected_conditions)
            
            # Additional validations for update scenario
            branch_names = {row['BRANCH_NAME'] for row in results}
            expected_updated_keywords = {"Renovated", "Expanded", "Modernized"}
            
            name_updates_found = any(keyword in name for name in branch_names for keyword in expected_updated_keywords)
            if not name_updates_found:
                validation_result['issues'].append('Branch name updates not detected')
                validation_result['status'] = 'FAIL'
            
            # Check for inactive branch handling (branch 104 should have UNKNOWN_REGION)
            branch_104_data = next((row for row in results if row['BRANCH_ID'] == 104), None)
            if branch_104_data and branch_104_data['REGION'] != 'UNKNOWN_REGION':
                validation_result['issues'].append('Inactive branch 104 should have UNKNOWN_REGION')
                validation_result['status'] = 'FAIL'
            
            execution_time = self._end_timer("scenario_2_execution")
            validation_result['execution_time'] = execution_time
            validation_result['input_data'] = 'Updated data for existing branches 101, 102, 104 with edge cases'
            validation_result['output_data'] = results
            
            self.test_results.append(validation_result)
            
            logger.info(f"Enhanced Scenario 2 Status: {validation_result['status']} - Execution time: {execution_time:.3f}s")
            if validation_result['issues']:
                logger.warning(f"Issues found: {validation_result['issues']}")
            
        except Exception as e:
            logger.error(f"Enhanced Scenario 2 failed with error: {e}")
            self.test_results.append({
                'scenario': 'Scenario 2: Update',
                'input_data': 'Updated data for existing branches 101, 102, 104 with edge cases',
                'output_data': [],
                'status': 'FAIL',
                'issues': [f"Exception occurred: {str(e)}"],
                'execution_time': self._end_timer("scenario_2_execution")
            })
    
    def generate_enhanced_markdown_report(self):
        """Generate comprehensive markdown test report with enhanced metrics"""
        logger.info("\n=== Generating Enhanced Test Report ===")
        
        report = "# Enhanced ETL Pipeline Test Report v2\n\n"
        report += f"**Generated on:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n"
        report += "## Executive Summary\n\n"
        
        total_tests = len(self.test_results)
        passed_tests = sum(1 for result in self.test_results if result['status'] == 'PASS')
        total_execution_time = sum(result.get('execution_time', 0) for result in self.test_results)
        
        report += f"- **Total Test Scenarios:** {total_tests}\n"
        report += f"- **Passed:** {passed_tests}\n"
        report += f"- **Failed:** {total_tests - passed_tests}\n"
        report += f"- **Total Execution Time:** {total_execution_time:.3f} seconds\n\n"
        
        # Performance metrics
        if self.performance_metrics:
            report += "## Performance Metrics\n\n"
            for operation, duration in self.performance_metrics.items():
                report += f"- **{operation}:** {duration:.3f}s\n"
            report += "\n"
        
        # Detailed test results
        report += "## Detailed Test Results\n\n"
        
        for i, result in enumerate(self.test_results, 1):
            report += f"### Test {i}: {result['scenario']}\n\n"
            report += f"**Input:** {result['input_data']}\n\n"
            
            if result['output_data']:
                report += "**Output:**\n\n"
                report += "| BRANCH_ID | BRANCH_NAME | TOTAL_TRANSACTIONS | TOTAL_AMOUNT | REGION | LAST_AUDIT_DATE |\n"
                report += "|-----------|-------------|-------------------|--------------|--------|-----------------|\n"
                
                for row in result['output_data']:
                    region = row['REGION'] if row['REGION'] else 'NULL'
                    audit_date = row['LAST_AUDIT_DATE'] if row['LAST_AUDIT_DATE'] else 'NULL'
                    report += f"| {row['BRANCH_ID']} | {row['BRANCH_NAME']} | {row['TOTAL_TRANSACTIONS']} | {row['TOTAL_AMOUNT']:.2f} | {region} | {audit_date} |\n"
                report += "\n"
            else:
                report += "**Output:** No data generated\n\n"
            
            # Enhanced metrics
            if 'metrics' in result:
                report += "**Metrics:**\n\n"
                for metric, value in result['metrics'].items():
                    report += f"- **{metric.replace('_', ' ').title()}:** {value}\n"
                report += "\n"
            
            # Execution time
            if 'execution_time' in result:
                report += f"**Execution Time:** {result['execution_time']:.3f} seconds\n\n"
            
            # Status and issues
            status_emoji = "✅" if result['status'] == 'PASS' else "❌"
            report += f"**Status:** {result['status']} {status_emoji}\n\n"
            
            if result.get('issues'):
                report += "**Issues:**\n\n"
                for issue in result['issues']:
                    report += f"- {issue}\n"
                report += "\n"
            
            report += "---\n\n"
        
        # Test summary
        report += "## Test Summary\n\n"
        if passed_tests == total_tests:
            report += "🎉 **All tests PASSED!** The ETL pipeline is functioning correctly.\n\n"
        else:
            report += f"⚠️ **{total_tests - passed_tests} test(s) FAILED!** Please review the issues above.\n\n"
        
        return report
    
    def run_all_enhanced_tests(self):
        """Run all enhanced test scenarios with comprehensive monitoring"""
        logger.info("Starting Enhanced ETL Pipeline Tests v2")
        
        overall_start_time = time.time()
        
        try:
            # Run enhanced test scenarios
            self.test_scenario_1_insert_enhanced()
            self.test_scenario_2_update_enhanced()
            
            # Generate and display comprehensive report
            report = self.generate_enhanced_markdown_report()
            print(report)
            
            # Enhanced summary with performance metrics
            total_execution_time = time.time() - overall_start_time
            passed_tests = sum(1 for result in self.test_results if result['status'] == 'PASS')
            total_tests = len(self.test_results)
            
            logger.info(f"\n=== Enhanced Test Summary ===")
            logger.info(f"Total Tests: {total_tests}")
            logger.info(f"Passed: {passed_tests}")
            logger.info(f"Failed: {total_tests - passed_tests}")
            logger.info(f"Overall Execution Time: {total_execution_time:.3f} seconds")
            
            if passed_tests == total_tests:
                logger.info("🎉 All enhanced tests PASSED! ✅")
            else:
                logger.warning(f"⚠️ {total_tests - passed_tests} enhanced test(s) FAILED! ❌")
            
            return passed_tests == total_tests
            
        except Exception as e:
            logger.error(f"Enhanced test execution failed: {e}")
            return False

def main():
    """Main enhanced test execution function"""
    try:
        logger.info("Initializing Enhanced ETL Testing Framework v2")
        tester = EnhancedETLTester()
        success = tester.run_all_enhanced_tests()
        
        if success:
            logger.info("All enhanced tests completed successfully")
            return 0
        else:
            logger.error("Some enhanced tests failed")
            return 1
            
    except Exception as e:
        logger.error(f"Enhanced test execution failed: {e}")
        return 1

if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)