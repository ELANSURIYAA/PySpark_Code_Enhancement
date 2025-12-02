_____________________________________________
## *Author*: AAVA
## *Created on*:   
## *Description*: Enhanced PySpark ETL pipeline for regulatory reporting with improved BRANCH_OPERATIONAL_DETAILS integration and performance optimizations
## *Version*: 2 
## *Changes*: Added performance optimizations, enhanced error handling, improved data validation, and better logging mechanisms
## *Reason*: User requested enhancements for better performance and reliability in production environments
## *Updated on*: 
_____________________________________________

import logging
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, count, sum as spark_sum, when, lit, broadcast, coalesce, current_timestamp
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DecimalType, DateType, LongType, DoubleType, TimestampType
from delta.tables import DeltaTable
from datetime import date, datetime
import time

# Configure logging with enhanced formatting
logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(name)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s'
)
logger = logging.getLogger(__name__)

class ETLPerformanceMonitor:
    """Class to monitor ETL performance and execution metrics"""
    
    def __init__(self):
        self.start_time = None
        self.step_times = {}
    
    def start_monitoring(self, step_name: str):
        """Start monitoring a specific step"""
        self.start_time = time.time()
        logger.info(f"Starting step: {step_name}")
    
    def end_monitoring(self, step_name: str):
        """End monitoring and log execution time"""
        if self.start_time:
            execution_time = time.time() - self.start_time
            self.step_times[step_name] = execution_time
            logger.info(f"Completed step: {step_name} in {execution_time:.2f} seconds")
            return execution_time
        return 0
    
    def get_summary(self):
        """Get performance summary"""
        total_time = sum(self.step_times.values())
        logger.info(f"Total ETL execution time: {total_time:.2f} seconds")
        for step, time_taken in self.step_times.items():
            logger.info(f"  - {step}: {time_taken:.2f}s ({(time_taken/total_time)*100:.1f}%)")
        return self.step_times

def get_spark_session(app_name: str = "RegulatoryReportingETL_Enhanced") -> SparkSession:
    """
    Initializes and returns a Spark session with enhanced configurations for better performance.
    """
    try:
        # Use getActiveSession() for Spark Connect compatibility
        spark = SparkSession.getActiveSession()
        if spark is None:
            spark = SparkSession.builder \
                .appName(app_name) \
                .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
                .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
                .config("spark.sql.adaptive.enabled", "true") \
                .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
                .config("spark.sql.adaptive.skewJoin.enabled", "true") \
                .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
                .getOrCreate()
        
        # Set log level for Spark logs
        spark.sparkContext.setLogLevel("WARN")
        logger.info("Enhanced Spark session created successfully with performance optimizations.")
        return spark
    except Exception as e:
        logger.error(f"Error creating Spark session: {e}")
        raise

def create_sample_data_with_validation(spark: SparkSession) -> tuple:
    """
    Creates sample data for testing the ETL pipeline with enhanced data validation.
    """
    logger.info("Creating enhanced sample data with validation checks")
    
    try:
        # Customer data with validation
        customer_schema = StructType([
            StructField("CUSTOMER_ID", IntegerType(), False),  # Not nullable
            StructField("NAME", StringType(), False),
            StructField("EMAIL", StringType(), True),
            StructField("PHONE", StringType(), True),
            StructField("ADDRESS", StringType(), True),
            StructField("CREATED_DATE", DateType(), True)
        ])
        
        customer_data = [
            (1, "John Doe", "john@email.com", "123-456-7890", "123 Main St", date(2023, 1, 15)),
            (2, "Jane Smith", "jane@email.com", "098-765-4321", "456 Oak Ave", date(2023, 2, 20)),
            (3, "Bob Johnson", "bob@email.com", "555-123-4567", "789 Pine Rd", date(2023, 3, 10)),
            (4, "Alice Brown", "alice@email.com", "444-555-6666", "321 Elm St", date(2023, 4, 5))
        ]
        
        customer_df = spark.createDataFrame(customer_data, customer_schema)
        
        # Branch data with enhanced structure
        branch_schema = StructType([
            StructField("BRANCH_ID", IntegerType(), False),
            StructField("BRANCH_NAME", StringType(), False),
            StructField("BRANCH_CODE", StringType(), True),
            StructField("CITY", StringType(), True),
            StructField("STATE", StringType(), True),
            StructField("COUNTRY", StringType(), True)
        ])
        
        branch_data = [
            (101, "Downtown Branch", "DT001", "New York", "NY", "USA"),
            (102, "Uptown Branch", "UT002", "Los Angeles", "CA", "USA"),
            (103, "Central Branch", "CT003", "Chicago", "IL", "USA"),
            (104, "Westside Branch", "WS004", "Houston", "TX", "USA")
        ]
        
        branch_df = spark.createDataFrame(branch_data, branch_schema)
        
        # Account data with enhanced validation
        account_schema = StructType([
            StructField("ACCOUNT_ID", IntegerType(), False),
            StructField("CUSTOMER_ID", IntegerType(), False),
            StructField("BRANCH_ID", IntegerType(), False),
            StructField("ACCOUNT_NUMBER", StringType(), False),
            StructField("ACCOUNT_TYPE", StringType(), False),
            StructField("BALANCE", DecimalType(15, 2), True),
            StructField("OPENED_DATE", DateType(), True)
        ])
        
        account_data = [
            (1001, 1, 101, "ACC001", "SAVINGS", 5000.00, date(2023, 1, 16)),
            (1002, 2, 102, "ACC002", "CHECKING", 2500.00, date(2023, 2, 21)),
            (1003, 3, 103, "ACC003", "SAVINGS", 7500.00, date(2023, 3, 11)),
            (1004, 1, 101, "ACC004", "CHECKING", 1200.00, date(2023, 4, 5)),
            (1005, 4, 104, "ACC005", "SAVINGS", 3200.00, date(2023, 4, 10))
        ]
        
        account_df = spark.createDataFrame(account_data, account_schema)
        
        # Transaction data with more comprehensive test cases
        transaction_schema = StructType([
            StructField("TRANSACTION_ID", IntegerType(), False),
            StructField("ACCOUNT_ID", IntegerType(), False),
            StructField("TRANSACTION_TYPE", StringType(), False),
            StructField("AMOUNT", DecimalType(15, 2), False),
            StructField("TRANSACTION_DATE", DateType(), True),
            StructField("DESCRIPTION", StringType(), True)
        ])
        
        transaction_data = [
            (10001, 1001, "DEPOSIT", 1000.00, date(2023, 5, 1), "Salary deposit"),
            (10002, 1001, "WITHDRAWAL", 200.00, date(2023, 5, 2), "ATM withdrawal"),
            (10003, 1002, "DEPOSIT", 500.00, date(2023, 5, 3), "Check deposit"),
            (10004, 1003, "TRANSFER", 300.00, date(2023, 5, 4), "Online transfer"),
            (10005, 1004, "DEPOSIT", 800.00, date(2023, 5, 5), "Cash deposit"),
            (10006, 1005, "DEPOSIT", 1500.00, date(2023, 5, 6), "Business deposit"),
            (10007, 1001, "TRANSFER", 250.00, date(2023, 5, 7), "Internal transfer"),
            (10008, 1002, "WITHDRAWAL", 100.00, date(2023, 5, 8), "Cash withdrawal")
        ]
        
        transaction_df = spark.createDataFrame(transaction_data, transaction_schema)
        
        # Enhanced Branch Operational Details with comprehensive test scenarios
        branch_operational_schema = StructType([
            StructField("BRANCH_ID", IntegerType(), False),
            StructField("REGION", StringType(), False),
            StructField("MANAGER_NAME", StringType(), True),
            StructField("LAST_AUDIT_DATE", DateType(), True),
            StructField("IS_ACTIVE", StringType(), False)
        ])
        
        branch_operational_data = [
            (101, "East Region", "Alice Manager", date(2023, 4, 15), "Y"),
            (102, "West Region", "Bob Manager", date(2023, 3, 20), "Y"),
            (103, "Central Region", "Charlie Manager", date(2023, 2, 10), "N"),  # Inactive branch
            (104, "South Region", "Diana Manager", date(2023, 4, 25), "Y")
        ]
        
        branch_operational_df = spark.createDataFrame(branch_operational_data, branch_operational_schema)
        
        # Add data validation logging
        logger.info(f"Created sample data - Customers: {customer_df.count()}, Branches: {branch_df.count()}, "
                   f"Accounts: {account_df.count()}, Transactions: {transaction_df.count()}, "
                   f"Branch Operations: {branch_operational_df.count()}")
        
        return customer_df, account_df, transaction_df, branch_df, branch_operational_df
        
    except Exception as e:
        logger.error(f"Error creating sample data: {e}")
        raise

def create_aml_customer_transactions_enhanced(customer_df: DataFrame, account_df: DataFrame, 
                                            transaction_df: DataFrame) -> DataFrame:
    """
    Creates the AML_CUSTOMER_TRANSACTIONS DataFrame with enhanced join strategies and validation.
    """
    logger.info("Creating enhanced AML Customer Transactions DataFrame with optimized joins.")
    
    try:
        # Use broadcast join for smaller customer table to optimize performance
        result_df = broadcast(customer_df).join(account_df, "CUSTOMER_ID", "inner") \
                                         .join(transaction_df, "ACCOUNT_ID", "inner") \
                                         .select(
                                             col("CUSTOMER_ID"),
                                             col("NAME"),
                                             col("ACCOUNT_ID"),
                                             col("TRANSACTION_ID"),
                                             col("AMOUNT"),
                                             col("TRANSACTION_TYPE"),
                                             col("TRANSACTION_DATE"),
                                             current_timestamp().alias("PROCESSED_TIMESTAMP")
                                         )
        
        # Add data quality checks
        total_records = result_df.count()
        logger.info(f"AML Customer Transactions created with {total_records} records")
        
        return result_df
        
    except Exception as e:
        logger.error(f"Error creating AML customer transactions: {e}")
        raise

def create_enhanced_branch_summary_report_v2(transaction_df: DataFrame, account_df: DataFrame, 
                                            branch_df: DataFrame, branch_operational_df: DataFrame) -> DataFrame:
    """
    Creates the enhanced BRANCH_SUMMARY_REPORT DataFrame with advanced BRANCH_OPERATIONAL_DETAILS integration,
    performance optimizations, and comprehensive error handling.
    """
    logger.info("Creating Enhanced Branch Summary Report v2 with advanced optimizations.")
    
    try:
        # Step 1: Create base branch summary with optimized aggregations
        logger.info("Step 1: Creating base branch summary with transaction aggregations")
        
        base_summary = transaction_df.join(account_df, "ACCOUNT_ID", "inner") \
                                     .join(broadcast(branch_df), "BRANCH_ID", "inner") \
                                     .groupBy("BRANCH_ID", "BRANCH_NAME") \
                                     .agg(
                                         count("*").alias("TOTAL_TRANSACTIONS"),
                                         spark_sum("AMOUNT").alias("TOTAL_AMOUNT"),
                                         count(col("TRANSACTION_ID").isNotNull()).alias("VALID_TRANSACTIONS")
                                     )
        
        logger.info(f"Base summary created with {base_summary.count()} branch records")
        
        # Step 2: Enhanced join with BRANCH_OPERATIONAL_DETAILS with comprehensive conditional logic
        logger.info("Step 2: Joining with BRANCH_OPERATIONAL_DETAILS and applying enhanced conditional logic")
        
        enhanced_summary = base_summary.join(branch_operational_df, "BRANCH_ID", "left") \
                                       .select(
                                           col("BRANCH_ID").cast(LongType()),
                                           col("BRANCH_NAME"),
                                           col("TOTAL_TRANSACTIONS"),
                                           col("TOTAL_AMOUNT").cast(DoubleType()),
                                           # Enhanced conditional population with coalesce for better null handling
                                           coalesce(
                                               when(col("IS_ACTIVE") == "Y", col("REGION")),
                                               lit("UNKNOWN_REGION")
                                           ).alias("REGION"),
                                           coalesce(
                                               when(col("IS_ACTIVE") == "Y", col("LAST_AUDIT_DATE").cast(StringType())),
                                               lit("NOT_AUDITED")
                                           ).alias("LAST_AUDIT_DATE"),
                                           # Additional metadata fields for enhanced reporting
                                           when(col("IS_ACTIVE") == "Y", lit("ACTIVE")).otherwise(lit("INACTIVE")).alias("BRANCH_STATUS"),
                                           col("VALID_TRANSACTIONS"),
                                           current_timestamp().alias("REPORT_GENERATED_TIMESTAMP")
                                       )
        
        # Step 3: Add data quality validations
        logger.info("Step 3: Performing data quality validations")
        
        # Check for data consistency
        total_branches = enhanced_summary.count()
        active_branches = enhanced_summary.filter(col("BRANCH_STATUS") == "ACTIVE").count()
        inactive_branches = enhanced_summary.filter(col("BRANCH_STATUS") == "INACTIVE").count()
        
        logger.info(f"Enhanced Branch Summary Report v2 created:")
        logger.info(f"  - Total branches: {total_branches}")
        logger.info(f"  - Active branches: {active_branches}")
        logger.info(f"  - Inactive branches: {inactive_branches}")
        
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

def write_to_delta_table_enhanced(df: DataFrame, table_name: str, mode: str = "overwrite", 
                                partition_cols: list = None):
    """
    Writes a DataFrame to a Delta table with enhanced configurations and error handling.
    """
    logger.info(f"Writing DataFrame to Delta table: {table_name} in {mode} mode with enhanced configurations")
    
    try:
        writer = df.write.format("delta") \
                   .mode(mode) \
                   .option("mergeSchema", "true") \
                   .option("autoOptimize.optimizeWrite", "true") \
                   .option("autoOptimize.autoCompact", "true")
        
        # Add partitioning if specified
        if partition_cols:
            writer = writer.partitionBy(*partition_cols)
            logger.info(f"Partitioning by columns: {partition_cols}")
        
        writer.saveAsTable(table_name)
        
        # Log success with record count
        record_count = df.count()
        logger.info(f"Successfully written {record_count} records to {table_name}")
        
    except Exception as e:
        logger.error(f"Failed to write to Delta table {table_name}: {e}")
        raise

def validate_data_quality_enhanced(df: DataFrame, table_name: str, 
                                 validation_rules: dict = None) -> dict:
    """
    Performs comprehensive data quality validations on the DataFrame.
    """
    logger.info(f"Performing enhanced data quality validation for {table_name}")
    
    validation_results = {
        "table_name": table_name,
        "total_records": 0,
        "validation_passed": True,
        "issues": []
    }
    
    try:
        # Basic validations
        row_count = df.count()
        validation_results["total_records"] = row_count
        
        if row_count == 0:
            validation_results["validation_passed"] = False
            validation_results["issues"].append("Table is empty")
            logger.warning(f"{table_name} is empty")
            return validation_results
        
        # Schema-specific validations
        if table_name == "BRANCH_SUMMARY_REPORT":
            # Check for null values in critical columns
            null_branch_ids = df.filter(col("BRANCH_ID").isNull()).count()
            null_branch_names = df.filter(col("BRANCH_NAME").isNull()).count()
            
            if null_branch_ids > 0:
                validation_results["validation_passed"] = False
                validation_results["issues"].append(f"Found {null_branch_ids} null BRANCH_ID values")
            
            if null_branch_names > 0:
                validation_results["validation_passed"] = False
                validation_results["issues"].append(f"Found {null_branch_names} null BRANCH_NAME values")
            
            # Check for negative amounts
            negative_amounts = df.filter(col("TOTAL_AMOUNT") < 0).count()
            if negative_amounts > 0:
                validation_results["issues"].append(f"Found {negative_amounts} records with negative amounts (warning)")
            
            # Check data distribution
            distinct_branches = df.select("BRANCH_ID").distinct().count()
            validation_results["distinct_branches"] = distinct_branches
            
        # Custom validation rules
        if validation_rules:
            for rule_name, rule_condition in validation_rules.items():
                try:
                    failed_records = df.filter(~rule_condition).count()
                    if failed_records > 0:
                        validation_results["issues"].append(f"Rule '{rule_name}' failed for {failed_records} records")
                except Exception as rule_error:
                    validation_results["issues"].append(f"Rule '{rule_name}' execution failed: {str(rule_error)}")
        
        if validation_results["validation_passed"]:
            logger.info(f"Data quality validation passed for {table_name} with {row_count} records")
        else:
            logger.warning(f"Data quality validation issues found for {table_name}: {validation_results['issues']}")
        
        return validation_results
        
    except Exception as e:
        logger.error(f"Data quality validation failed for {table_name}: {e}")
        validation_results["validation_passed"] = False
        validation_results["issues"].append(f"Validation execution error: {str(e)}")
        return validation_results

def main():
    """
    Enhanced main ETL execution function with comprehensive monitoring, error handling, and performance optimizations.
    """
    spark = None
    monitor = ETLPerformanceMonitor()
    
    try:
        logger.info("Starting Enhanced Regulatory Reporting ETL v2")
        
        # Initialize Spark session
        monitor.start_monitoring("spark_initialization")
        spark = get_spark_session()
        monitor.end_monitoring("spark_initialization")
        
        # Create sample data
        monitor.start_monitoring("data_creation")
        customer_df, account_df, transaction_df, branch_df, branch_operational_df = create_sample_data_with_validation(spark)
        monitor.end_monitoring("data_creation")
        
        logger.info("Enhanced sample data created successfully")
        
        # Process AML_CUSTOMER_TRANSACTIONS
        monitor.start_monitoring("aml_processing")
        aml_transactions_df = create_aml_customer_transactions_enhanced(customer_df, account_df, transaction_df)
        
        # Validate AML data
        aml_validation = validate_data_quality_enhanced(aml_transactions_df, "AML_CUSTOMER_TRANSACTIONS")
        
        if aml_validation["validation_passed"]:
            write_to_delta_table_enhanced(aml_transactions_df, "AML_CUSTOMER_TRANSACTIONS")
        else:
            logger.error(f"AML data validation failed: {aml_validation['issues']}")
            raise ValueError("AML data quality validation failed")
        
        monitor.end_monitoring("aml_processing")
        
        # Process enhanced BRANCH_SUMMARY_REPORT
        monitor.start_monitoring("branch_summary_processing")
        enhanced_branch_summary_df = create_enhanced_branch_summary_report_v2(
            transaction_df, account_df, branch_df, branch_operational_df
        )
        
        # Define custom validation rules for branch summary
        branch_validation_rules = {
            "positive_transactions": col("TOTAL_TRANSACTIONS") >= 0,
            "valid_branch_id": col("BRANCH_ID").isNotNull() & (col("BRANCH_ID") > 0)
        }
        
        # Validate branch summary data
        branch_validation = validate_data_quality_enhanced(
            enhanced_branch_summary_df, 
            "BRANCH_SUMMARY_REPORT", 
            branch_validation_rules
        )
        
        if branch_validation["validation_passed"]:
            write_to_delta_table_enhanced(enhanced_branch_summary_df, "workspace.default.branch_summary_report")
        else:
            logger.error(f"Branch summary data validation failed: {branch_validation['issues']}")
            raise ValueError("Branch summary data quality validation failed")
        
        monitor.end_monitoring("branch_summary_processing")
        
        # Display enhanced results
        logger.info("Displaying enhanced results with comprehensive data:")
        print("\n=== Enhanced Branch Summary Report v2 Sample ===")
        enhanced_branch_summary_df.show(truncate=False)
        
        # Performance summary
        monitor.get_summary()
        
        # Final validation summary
        logger.info("\n=== Data Quality Summary ===")
        logger.info(f"AML Transactions: {aml_validation['total_records']} records - {'PASSED' if aml_validation['validation_passed'] else 'FAILED'}")
        logger.info(f"Branch Summary: {branch_validation['total_records']} records - {'PASSED' if branch_validation['validation_passed'] else 'FAILED'}")
        
        logger.info("Enhanced ETL job v2 completed successfully with comprehensive BRANCH_OPERATIONAL_DETAILS integration.")
        
    except Exception as e:
        logger.error(f"Enhanced ETL job v2 failed with exception: {e}")
        raise
    finally:
        if spark:
            logger.info("Enhanced ETL job v2 execution completed.")

if __name__ == "__main__":
    main()