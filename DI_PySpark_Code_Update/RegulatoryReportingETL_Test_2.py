# _____________________________________________
# ## *Author*: AAVA
# ## *Created on*:   
# ## *Description*: Enhanced Python-based test script for RegulatoryReportingETL Pipeline v2 with comprehensive test scenarios
# ## *Version*: 2 
# ## *Updated on*: 
# ## *Changes*: Added comprehensive test scenarios, enhanced validation logic, performance testing, and detailed reporting
# ## *Reason*: User requested enhanced testing capabilities with better coverage and validation
# _____________________________________________

import logging
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, count, sum as spark_sum, when, lit, broadcast, coalesce
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DecimalType, DateType, LongType, DoubleType
import sys
from datetime import datetime
from typing import Dict, List, Any, Tuple
import time

# Configure enhanced logging
logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(name)s - %(levelname)s - [%(funcName)s:%(lineno)d] - %(message)s'
)
logger = logging.getLogger(__name__)

class EnhancedTestRegulatoryReportingETL:
    """
    Enhanced Test class for RegulatoryReportingETL Pipeline v2.
    Tests multiple scenarios including insert, update, edge cases, and performance validation.
    """
    
    def __init__(self):
        self.spark = self._get_spark_session()
        self.test_results = []
        self.performance_metrics = {}
        self.start_time = time.time()
    
    def _get_spark_session(self) -> SparkSession:
        """
        Initialize enhanced Spark session for testing with optimized configurations.
        """
        try:
            spark = SparkSession.getActiveSession()
            if spark is None:
                spark = SparkSession.builder \
                    .appName("RegulatoryReportingETL_Enhanced_Test_v2") \
                    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
                    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
                    .config("spark.sql.adaptive.enabled", "true") \
                    .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
                    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
                    .getOrCreate()
            
            logger.info("Enhanced Spark session initialized for testing")
            return spark
        except Exception as e:
            logger.error(f"Error creating Spark session: {e}")
            raise
    
    def _record_performance_metric(self, operation: str, duration: float, record_count: int = 0):
        """
        Record performance metrics for analysis.
        """
        self.performance_metrics[operation] = {
            'duration_seconds': duration,
            'record_count': record_count,
            'records_per_second': record_count / duration if duration > 0 else 0
        }
    
    def create_test_data_scenario1_enhanced(self) -> Tuple[DataFrame, DataFrame, DataFrame, DataFrame, DataFrame]:
        """
        Enhanced test data for Scenario 1: Insert new records with comprehensive data validation.
        """
        logger.info("Creating enhanced test data for Scenario 1: Insert with validation")
        
        start_time = time.time()
        
        # Enhanced Customer data with validation scenarios
        customer_schema = StructType([
            StructField("CUSTOMER_ID", IntegerType(), False),
            StructField("NAME", StringType(), False),
            StructField("EMAIL", StringType(), True),
            StructField("PHONE", StringType(), True),
            StructField("ADDRESS", StringType(), True),
            StructField("CREATED_DATE", DateType(), True)
        ])
        
        customer_data = [
            (1, "Alice Johnson", "alice.johnson@email.com", "111-222-3333", "100 First St", "2023-01-10"),
            (2, "Bob Smith", "bob.smith@email.com", "444-555-6666", "200 Second Ave", "2023-02-15"),
            (3, "Carol Williams", "carol.williams@email.com", "777-888-9999", "300 Third Blvd", "2023-03-20")
        ]
        customer_df = self.spark.createDataFrame(customer_data, customer_schema).cache()
        
        # Enhanced Branch data
        branch_schema = StructType([
            StructField("BRANCH_ID", IntegerType(), False),
            StructField("BRANCH_NAME", StringType(), False),
            StructField("BRANCH_CODE", StringType(), True),
            StructField("CITY", StringType(), True),
            StructField("STATE", StringType(), True),
            StructField("COUNTRY", StringType(), True)
        ])
        
        branch_data = [
            (201, "North Branch", "NB201", "Boston", "MA", "USA"),
            (202, "South Branch", "SB202", "Miami", "FL", "USA"),
            (203, "East Branch", "EB203", "Atlanta", "GA", "USA")
        ]
        branch_df = self.spark.createDataFrame(branch_data, branch_schema).cache()
        
        # Enhanced Account data
        account_schema = StructType([
            StructField("ACCOUNT_ID", IntegerType(), False),
            StructField("CUSTOMER_ID", IntegerType(), False),
            StructField("BRANCH_ID", IntegerType(), False),
            StructField("ACCOUNT_NUMBER", StringType(), False),
            StructField("ACCOUNT_TYPE", StringType(), True),
            StructField("BALANCE", DecimalType(15,2), True),
            StructField("OPENED_DATE", DateType(), True)
        ])
        
        account_data = [
            (2001, 1, 201, "ACC2001", "SAVINGS", 10000.00, "2023-01-15"),
            (2002, 2, 202, "ACC2002", "CHECKING", 5000.00, "2023-02-20"),
            (2003, 3, 203, "ACC2003", "SAVINGS", 15000.00, "2023-03-25"),
            (2004, 1, 201, "ACC2004", "CHECKING", 3000.00, "2023-04-01")
        ]
        account_df = self.spark.createDataFrame(account_data, account_schema).cache()
        
        # Enhanced Transaction data with diverse scenarios
        transaction_schema = StructType([
            StructField("TRANSACTION_ID", IntegerType(), False),
            StructField("ACCOUNT_ID", IntegerType(), False),
            StructField("TRANSACTION_TYPE", StringType(), False),
            StructField("AMOUNT", DecimalType(15,2), False),
            StructField("TRANSACTION_DATE", DateType(), True),
            StructField("DESCRIPTION", StringType(), True)
        ])
        
        transaction_data = [
            (20001, 2001, "DEPOSIT", 2000.00, "2023-05-01", "Initial deposit"),
            (20002, 2001, "WITHDRAWAL", 500.00, "2023-05-02", "ATM withdrawal"),
            (20003, 2002, "DEPOSIT", 1000.00, "2023-05-03", "Payroll deposit"),
            (20004, 2002, "TRANSFER", 300.00, "2023-05-04", "Online transfer"),
            (20005, 2003, "DEPOSIT", 2500.00, "2023-05-05", "Investment deposit"),
            (20006, 2003, "WITHDRAWAL", 200.00, "2023-05-06", "Cash withdrawal"),
            (20007, 2004, "DEPOSIT", 800.00, "2023-05-07", "Bonus deposit")
        ]
        transaction_df = self.spark.createDataFrame(transaction_data, transaction_schema)
        
        # Enhanced Branch Operational Details - ALL ACTIVE for insert scenario
        branch_operational_schema = StructType([
            StructField("BRANCH_ID", IntegerType(), False),
            StructField("REGION", StringType(), False),
            StructField("MANAGER_NAME", StringType(), True),
            StructField("LAST_AUDIT_DATE", DateType(), True),
            StructField("IS_ACTIVE", StringType(), False)
        ])
        
        branch_operational_data = [
            (201, "Northeast Region", "John Manager", "2023-04-01", "Y"),
            (202, "Southeast Region", "Jane Manager", "2023-03-15", "Y"),
            (203, "Southeast Region", "Mike Manager", "2023-04-10", "Y")
        ]
        branch_operational_df = self.spark.createDataFrame(branch_operational_data, branch_operational_schema).cache()
        
        duration = time.time() - start_time
        self._record_performance_metric("create_test_data_scenario1", duration, transaction_df.count())
        
        return customer_df, account_df, transaction_df, branch_df, branch_operational_df
    
    def create_test_data_scenario2_enhanced(self) -> Tuple[DataFrame, DataFrame, DataFrame, DataFrame, DataFrame]:
        """
        Enhanced test data for Scenario 2: Update existing records with mixed active/inactive status.
        """
        logger.info("Creating enhanced test data for Scenario 2: Update with mixed status")
        
        start_time = time.time()
        
        # Reuse base data structure
        customer_df, account_df, base_transaction_df, branch_df, _ = self.create_test_data_scenario1_enhanced()
        
        # Additional transactions to simulate updates
        transaction_schema = StructType([
            StructField("TRANSACTION_ID", IntegerType(), False),
            StructField("ACCOUNT_ID", IntegerType(), False),
            StructField("TRANSACTION_TYPE", StringType(), False),
            StructField("AMOUNT", DecimalType(15,2), False),
            StructField("TRANSACTION_DATE", DateType(), True),
            StructField("DESCRIPTION", StringType(), True)
        ])
        
        additional_transaction_data = [
            (20008, 2001, "DEPOSIT", 1500.00, "2023-05-08", "Bonus deposit"),
            (20009, 2002, "WITHDRAWAL", 200.00, "2023-05-09", "Cash withdrawal"),
            (20010, 2003, "TRANSFER", 500.00, "2023-05-10", "Investment transfer"),
            (20011, 2004, "DEPOSIT", 1000.00, "2023-05-11", "Salary deposit")
        ]
        additional_transaction_df = self.spark.createDataFrame(additional_transaction_data, transaction_schema)
        
        # Union transactions to simulate cumulative updates
        updated_transaction_df = base_transaction_df.union(additional_transaction_df)
        
        # Updated Branch Operational Details - MIXED ACTIVE/INACTIVE STATUS
        branch_operational_schema = StructType([
            StructField("BRANCH_ID", IntegerType(), False),
            StructField("REGION", StringType(), False),
            StructField("MANAGER_NAME", StringType(), True),
            StructField("LAST_AUDIT_DATE", DateType(), True),
            StructField("IS_ACTIVE", StringType(), False)
        ])
        
        updated_branch_operational_data = [
            (201, "Northeast Region", "John Manager", "2023-05-01", "Y"),  # Active - should have region
            (202, "Southeast Region", "Jane Manager", "2023-04-20", "N"),  # Inactive - should NOT have region
            (203, "Southeast Region", "Mike Manager", "2023-05-05", "Y")   # Active - should have region
        ]
        updated_branch_operational_df = self.spark.createDataFrame(updated_branch_operational_data, branch_operational_schema).cache()
        
        duration = time.time() - start_time
        self._record_performance_metric("create_test_data_scenario2", duration, updated_transaction_df.count())
        
        return customer_df, account_df, updated_transaction_df, branch_df, updated_branch_operational_df
    
    def create_test_data_scenario3_edge_cases(self) -> Tuple[DataFrame, DataFrame, DataFrame, DataFrame, DataFrame]:
        """
        Create test data for Scenario 3: Edge cases including missing operational details and null handling.
        """
        logger.info("Creating test data for Scenario 3: Edge cases and null handling")
        
        # Base data with additional edge case branch
        customer_df, account_df, transaction_df, base_branch_df, _ = self.create_test_data_scenario1_enhanced()
        
        # Add branch without operational details
        branch_schema = StructType([
            StructField("BRANCH_ID", IntegerType(), False),
            StructField("BRANCH_NAME", StringType(), False),
            StructField("BRANCH_CODE", StringType(), True),
            StructField("CITY", StringType(), True),
            StructField("STATE", StringType(), True),
            StructField("COUNTRY", StringType(), True)
        ])
        
        additional_branch_data = [(204, "Orphan Branch", "OR204", "Denver", "CO", "USA")]
        additional_branch_df = self.spark.createDataFrame(additional_branch_data, branch_schema)
        extended_branch_df = base_branch_df.union(additional_branch_df).cache()
        
        # Add account and transactions for orphan branch
        account_schema = StructType([
            StructField("ACCOUNT_ID", IntegerType(), False),
            StructField("CUSTOMER_ID", IntegerType(), False),
            StructField("BRANCH_ID", IntegerType(), False),
            StructField("ACCOUNT_NUMBER", StringType(), False),
            StructField("ACCOUNT_TYPE", StringType(), True),
            StructField("BALANCE", DecimalType(15,2), True),
            StructField("OPENED_DATE", DateType(), True)
        ])
        
        additional_account_data = [(2005, 1, 204, "ACC2005", "SAVINGS", 8000.00, "2023-04-15")]
        additional_account_df = self.spark.createDataFrame(additional_account_data, account_schema)
        extended_account_df = account_df.union(additional_account_df).cache()
        
        # Add transactions for orphan branch
        transaction_schema = StructType([
            StructField("TRANSACTION_ID", IntegerType(), False),
            StructField("ACCOUNT_ID", IntegerType(), False),
            StructField("TRANSACTION_TYPE", StringType(), False),
            StructField("AMOUNT", DecimalType(15,2), False),
            StructField("TRANSACTION_DATE", DateType(), True),
            StructField("DESCRIPTION", StringType(), True)
        ])
        
        additional_transaction_data = [
            (20012, 2005, "DEPOSIT", 1200.00, "2023-05-12", "Orphan branch deposit")
        ]
        additional_transaction_df = self.spark.createDataFrame(additional_transaction_data, transaction_schema)
        extended_transaction_df = transaction_df.union(additional_transaction_df)
        
        # Operational details - MISSING branch 204 (orphan branch)
        branch_operational_schema = StructType([
            StructField("BRANCH_ID", IntegerType(), False),
            StructField("REGION", StringType(), False),
            StructField("MANAGER_NAME", StringType(), True),
            StructField("LAST_AUDIT_DATE", DateType(), True),
            StructField("IS_ACTIVE", StringType(), False)
        ])
        
        # Note: Branch 204 is intentionally missing from operational details
        branch_operational_data = [
            (201, "Northeast Region", "John Manager", "2023-04-01", "Y"),
            (202, "Southeast Region", "Jane Manager", "2023-03-15", "Y"),
            (203, "Southeast Region", "Mike Manager", "2023-04-10", "N")  # Inactive
            # Branch 204 missing - should result in NULL region and audit date
        ]
        branch_operational_df = self.spark.createDataFrame(branch_operational_data, branch_operational_schema).cache()
        
        return customer_df, extended_account_df, extended_transaction_df, extended_branch_df, branch_operational_df
    
    def create_enhanced_branch_summary_report_v2(self, transaction_df: DataFrame, account_df: DataFrame, 
                                                branch_df: DataFrame, branch_operational_df: DataFrame) -> DataFrame:
        """
        Replicated enhanced logic from main pipeline v2 for testing.
        """
        start_time = time.time()
        
        # Step 1: Create base branch summary with optimized joins
        base_summary = transaction_df \
            .join(account_df, "ACCOUNT_ID") \
            .join(broadcast(branch_df), "BRANCH_ID") \
            .groupBy("BRANCH_ID", "BRANCH_NAME") \
            .agg(
                count("*").alias("TOTAL_TRANSACTIONS"),
                spark_sum("AMOUNT").alias("TOTAL_AMOUNT")
            ) \
            .filter(col("TOTAL_TRANSACTIONS") > 0)
        
        # Step 2: Enhanced join with operational details
        enhanced_summary = base_summary.join(
            broadcast(branch_operational_df), 
            base_summary["BRANCH_ID"] == branch_operational_df["BRANCH_ID"], 
            "left"
        ).select(
            base_summary["BRANCH_ID"].cast(LongType()).alias("BRANCH_ID"),
            base_summary["BRANCH_NAME"],
            base_summary["TOTAL_TRANSACTIONS"],
            base_summary["TOTAL_AMOUNT"].cast(DoubleType()).alias("TOTAL_AMOUNT"),
            when(coalesce(branch_operational_df["IS_ACTIVE"], lit("N")) == "Y", 
                 branch_operational_df["REGION"]).alias("REGION"),
            when(coalesce(branch_operational_df["IS_ACTIVE"], lit("N")) == "Y", 
                 branch_operational_df["LAST_AUDIT_DATE"].cast(StringType())).alias("LAST_AUDIT_DATE")
        ).orderBy("BRANCH_ID")
        
        duration = time.time() - start_time
        self._record_performance_metric("create_branch_summary", duration, enhanced_summary.count())
        
        return enhanced_summary
    
    def validate_test_results(self, result_df: DataFrame, expected_conditions: Dict[str, Any]) -> Dict[str, Any]:
        """
        Enhanced validation logic with comprehensive checks.
        """
        validation_results = {
            'passed': True,
            'details': {},
            'errors': []
        }
        
        try:
            results = result_df.collect()
            
            # Check expected record count
            if 'expected_record_count' in expected_conditions:
                actual_count = len(results)
                expected_count = expected_conditions['expected_record_count']
                validation_results['details']['record_count_check'] = {
                    'expected': expected_count,
                    'actual': actual_count,
                    'passed': actual_count == expected_count
                }
                if actual_count != expected_count:
                    validation_results['passed'] = False
                    validation_results['errors'].append(f"Record count mismatch: expected {expected_count}, got {actual_count}")
            
            # Check branches with regions (active branches)
            if 'expected_active_branches' in expected_conditions:
                active_branches = [row for row in results if row['REGION'] is not None]
                expected_active = expected_conditions['expected_active_branches']
                actual_active = len(active_branches)
                validation_results['details']['active_branches_check'] = {
                    'expected': expected_active,
                    'actual': actual_active,
                    'passed': actual_active == expected_active
                }
                if actual_active != expected_active:
                    validation_results['passed'] = False
                    validation_results['errors'].append(f"Active branches mismatch: expected {expected_active}, got {actual_active}")
            
            # Check specific branch conditions
            if 'branch_conditions' in expected_conditions:
                for branch_id, conditions in expected_conditions['branch_conditions'].items():
                    branch_result = next((row for row in results if row['BRANCH_ID'] == branch_id), None)
                    if branch_result is None:
                        validation_results['passed'] = False
                        validation_results['errors'].append(f"Branch {branch_id} not found in results")
                        continue
                    
                    for condition, expected_value in conditions.items():
                        actual_value = branch_result[condition]
                        if condition == 'has_region':
                            actual_value = actual_value is not None
                        
                        validation_results['details'][f'branch_{branch_id}_{condition}'] = {
                            'expected': expected_value,
                            'actual': actual_value,
                            'passed': actual_value == expected_value
                        }
                        
                        if actual_value != expected_value:
                            validation_results['passed'] = False
                            validation_results['errors'].append(
                                f"Branch {branch_id} {condition} mismatch: expected {expected_value}, got {actual_value}"
                            )
            
            # Check data quality
            negative_amounts = [row for row in results if row['TOTAL_AMOUNT'] < 0]
            if negative_amounts:
                validation_results['passed'] = False
                validation_results['errors'].append(f"Found {len(negative_amounts)} branches with negative amounts")
            
            zero_transactions = [row for row in results if row['TOTAL_TRANSACTIONS'] <= 0]
            if zero_transactions:
                validation_results['passed'] = False
                validation_results['errors'].append(f"Found {len(zero_transactions)} branches with zero or negative transactions")
            
        except Exception as e:
            validation_results['passed'] = False
            validation_results['errors'].append(f"Validation error: {str(e)}")
        
        return validation_results
    
    def test_scenario_1_insert_enhanced(self):
        """
        Enhanced Test Scenario 1: Insert new records with comprehensive validation.
        """
        logger.info("\n=== Testing Enhanced Scenario 1: Insert ===")
        
        try:
            # Create test data
            customer_df, account_df, transaction_df, branch_df, branch_operational_df = self.create_test_data_scenario1_enhanced()
            
            # Execute pipeline logic
            result_df = self.create_enhanced_branch_summary_report_v2(
                transaction_df, account_df, branch_df, branch_operational_df
            )
            
            # Define expected conditions
            expected_conditions = {
                'expected_record_count': 3,
                'expected_active_branches': 3,  # All branches are active
                'branch_conditions': {
                    201: {'has_region': True, 'TOTAL_TRANSACTIONS': 2},
                    202: {'has_region': True, 'TOTAL_TRANSACTIONS': 2},
                    203: {'has_region': True, 'TOTAL_TRANSACTIONS': 2}
                }
            }
            
            # Validate results
            validation_results = self.validate_test_results(result_df, expected_conditions)
            
            self.test_results.append({
                'scenario': 'Enhanced Scenario 1: Insert',
                'status': 'PASS' if validation_results['passed'] else 'FAIL',
                'input_data': {
                    'branches': branch_df.count(),
                    'transactions': transaction_df.count(),
                    'active_operational_details': 3
                },
                'output_data': result_df.collect(),
                'validation': validation_results,
                'performance': self.performance_metrics.get('create_branch_summary', {})
            })
            
            logger.info(f"Enhanced Scenario 1 Test Result: {'PASS' if validation_results['passed'] else 'FAIL'}")
            return validation_results['passed']
            
        except Exception as e:
            logger.error(f"Enhanced Scenario 1 test failed with exception: {e}")
            self.test_results.append({
                'scenario': 'Enhanced Scenario 1: Insert',
                'status': 'FAIL',
                'error': str(e)
            })
            return False
    
    def test_scenario_2_update_enhanced(self):
        """
        Enhanced Test Scenario 2: Update with mixed active/inactive status.
        """
        logger.info("\n=== Testing Enhanced Scenario 2: Update with Mixed Status ===")
        
        try:
            # Create test data with updates
            customer_df, account_df, transaction_df, branch_df, branch_operational_df = self.create_test_data_scenario2_enhanced()
            
            # Execute pipeline logic
            result_df = self.create_enhanced_branch_summary_report_v2(
                transaction_df, account_df, branch_df, branch_operational_df
            )
            
            # Define expected conditions
            expected_conditions = {
                'expected_record_count': 3,
                'expected_active_branches': 2,  # Only branches 201 and 203 are active
                'branch_conditions': {
                    201: {'has_region': True, 'TOTAL_TRANSACTIONS': 3},   # Active
                    202: {'has_region': False, 'TOTAL_TRANSACTIONS': 3},  # Inactive
                    203: {'has_region': True, 'TOTAL_TRANSACTIONS': 3}    # Active
                }
            }
            
            # Validate results
            validation_results = self.validate_test_results(result_df, expected_conditions)
            
            self.test_results.append({
                'scenario': 'Enhanced Scenario 2: Update',
                'status': 'PASS' if validation_results['passed'] else 'FAIL',
                'input_data': {
                    'branches': branch_df.count(),
                    'transactions': transaction_df.count(),
                    'active_operational_details': 2,
                    'inactive_operational_details': 1
                },
                'output_data': result_df.collect(),
                'validation': validation_results,
                'performance': self.performance_metrics.get('create_branch_summary', {})
            })
            
            logger.info(f"Enhanced Scenario 2 Test Result: {'PASS' if validation_results['passed'] else 'FAIL'}")
            return validation_results['passed']
            
        except Exception as e:
            logger.error(f"Enhanced Scenario 2 test failed with exception: {e}")
            self.test_results.append({
                'scenario': 'Enhanced Scenario 2: Update',
                'status': 'FAIL',
                'error': str(e)
            })
            return False
    
    def test_scenario_3_edge_cases(self):
        """
        Test Scenario 3: Edge cases including missing operational details.
        """
        logger.info("\n=== Testing Scenario 3: Edge Cases and Null Handling ===")
        
        try:
            # Create edge case test data
            customer_df, account_df, transaction_df, branch_df, branch_operational_df = self.create_test_data_scenario3_edge_cases()
            
            # Execute pipeline logic
            result_df = self.create_enhanced_branch_summary_report_v2(
                transaction_df, account_df, branch_df, branch_operational_df
            )
            
            # Define expected conditions
            expected_conditions = {
                'expected_record_count': 4,  # Including orphan branch
                'expected_active_branches': 2,  # Only branches 201 and 202 are active (203 is inactive, 204 missing)
                'branch_conditions': {
                    201: {'has_region': True},   # Active
                    202: {'has_region': True},   # Active
                    203: {'has_region': False},  # Inactive
                    204: {'has_region': False}   # Missing operational details
                }
            }
            
            # Validate results
            validation_results = self.validate_test_results(result_df, expected_conditions)
            
            self.test_results.append({
                'scenario': 'Scenario 3: Edge Cases',
                'status': 'PASS' if validation_results['passed'] else 'FAIL',
                'input_data': {
                    'branches': branch_df.count(),
                    'transactions': transaction_df.count(),
                    'branches_with_operational_details': branch_operational_df.count(),
                    'orphan_branches': 1
                },
                'output_data': result_df.collect(),
                'validation': validation_results,
                'performance': self.performance_metrics.get('create_branch_summary', {})
            })
            
            logger.info(f"Scenario 3 Test Result: {'PASS' if validation_results['passed'] else 'FAIL'}")
            return validation_results['passed']
            
        except Exception as e:
            logger.error(f"Scenario 3 test failed with exception: {e}")
            self.test_results.append({
                'scenario': 'Scenario 3: Edge Cases',
                'status': 'FAIL',
                'error': str(e)
            })
            return False
    
    def generate_enhanced_markdown_report(self) -> str:
        """
        Generate an enhanced markdown report with performance metrics and detailed analysis.
        """
        total_duration = time.time() - self.start_time
        
        report = "# Enhanced Test Report v2\n\n"
        report += f"**Test Execution Time:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
        report += f"**Total Execution Duration:** {total_duration:.2f} seconds\n\n"
        
        # Performance Summary
        report += "## Performance Summary\n\n"
        if self.performance_metrics:
            report += "| Operation | Duration (s) | Records | Records/sec |\n"
            report += "|-----------|--------------|---------|-------------|\n"
            for operation, metrics in self.performance_metrics.items():
                report += f"| {operation} | {metrics['duration_seconds']:.3f} | {metrics['record_count']} | {metrics['records_per_second']:.1f} |\n"
        else:
            report += "No performance metrics recorded.\n"
        report += "\n"
        
        # Test Results
        for result in self.test_results:
            report += f"## {result['scenario']}\n\n"
            
            if 'error' in result:
                report += f"**Status:** {result['status']}\n"
                report += f"**Error:** {result['error']}\n\n"
                continue
            
            # Input section
            report += "### Input Data:\n"
            input_data = result['input_data']
            for key, value in input_data.items():
                report += f"- {key.replace('_', ' ').title()}: {value}\n"
            report += "\n"
            
            # Performance metrics for this scenario
            if 'performance' in result and result['performance']:
                perf = result['performance']
                report += "### Performance:\n"
                report += f"- Duration: {perf.get('duration_seconds', 0):.3f} seconds\n"
                report += f"- Records Processed: {perf.get('record_count', 0)}\n"
                report += f"- Processing Rate: {perf.get('records_per_second', 0):.1f} records/sec\n\n"
            
            # Output section
            report += "### Output:\n"
            report += "| BRANCH_ID | BRANCH_NAME | TOTAL_TRANSACTIONS | TOTAL_AMOUNT | REGION | LAST_AUDIT_DATE |\n"
            report += "|-----------|-------------|-------------------|--------------|--------|----------------|\n"
            
            for row in result['output_data']:
                region = row['REGION'] if row['REGION'] else 'NULL'
                audit_date = row['LAST_AUDIT_DATE'] if row['LAST_AUDIT_DATE'] else 'NULL'
                report += f"| {row['BRANCH_ID']} | {row['BRANCH_NAME']} | {row['TOTAL_TRANSACTIONS']} | {row['TOTAL_AMOUNT']:.2f} | {region} | {audit_date} |\n"
            
            report += "\n"
            
            # Validation Details
            if 'validation' in result:
                validation = result['validation']
                report += "### Validation Results:\n"
                report += f"**Overall Status:** {'✅ PASS' if validation['passed'] else '❌ FAIL'}\n\n"
                
                if validation['details']:
                    report += "#### Detailed Checks:\n"
                    for check_name, check_result in validation['details'].items():
                        status_icon = "✅" if check_result['passed'] else "❌"
                        report += f"- {status_icon} {check_name.replace('_', ' ').title()}: Expected {check_result['expected']}, Got {check_result['actual']}\n"
                    report += "\n"
                
                if validation['errors']:
                    report += "#### Validation Errors:\n"
                    for error in validation['errors']:
                        report += f"- ❌ {error}\n"
                    report += "\n"
            
            # Status
            status_icon = "✅" if result['status'] == 'PASS' else "❌"
            report += f"**Final Status:** {status_icon} {result['status']}\n\n"
            report += "---\n\n"
        
        return report
    
    def run_all_enhanced_tests(self):
        """
        Run all enhanced test scenarios and generate comprehensive report.
        """
        logger.info("Starting Enhanced RegulatoryReportingETL Test Suite v2")
        
        # Run all test scenarios
        scenario1_passed = self.test_scenario_1_insert_enhanced()
        scenario2_passed = self.test_scenario_2_update_enhanced()
        scenario3_passed = self.test_scenario_3_edge_cases()
        
        # Generate and print enhanced report
        report = self.generate_enhanced_markdown_report()
        print("\n" + "="*100)
        print("ENHANCED TEST EXECUTION REPORT v2")
        print("="*100)
        print(report)
        
        # Summary
        total_tests = len(self.test_results)
        passed_tests = sum(1 for result in self.test_results if result['status'] == 'PASS')
        
        logger.info(f"Enhanced Test Summary: {passed_tests}/{total_tests} tests passed")
        logger.info(f"Overall Success Rate: {(passed_tests/total_tests)*100:.1f}%")
        
        return scenario1_passed and scenario2_passed and scenario3_passed

def main():
    """
    Main enhanced test execution function.
    """
    try:
        logger.info("Initializing Enhanced Test Suite v2")
        test_suite = EnhancedTestRegulatoryReportingETL()
        all_tests_passed = test_suite.run_all_enhanced_tests()
        
        if all_tests_passed:
            logger.info("🎉 All enhanced tests passed successfully!")
            return 0
        else:
            logger.error("❌ Some enhanced tests failed!")
            return 1
            
    except Exception as e:
        logger.error(f"Enhanced test execution failed: {e}")
        return 1

if __name__ == "__main__":
    exit_code = main()
    # In Databricks, we don't typically use sys.exit()
    if exit_code == 0:
        logger.info("Enhanced test suite completed successfully")
    else:
        logger.error(f"Enhanced test suite failed with exit code: {exit_code}")