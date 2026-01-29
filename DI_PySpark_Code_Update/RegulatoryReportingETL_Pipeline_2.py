# _____________________________________________
# ## *Author*: AAVA
# ## *Created on*:   
# ## *Description*: Enhanced PySpark ETL pipeline for regulatory reporting with BRANCH_OPERATIONAL_DETAILS integration and performance optimizations
# ## *Version*: 2 
# ## *Updated on*: 
# ## *Changes*: Added performance optimizations, enhanced error handling, improved data validation, and better memory management
# ## *Reason*: User requested new enhancements for better performance and reliability
# _____________________________________________

import logging
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, count, sum as spark_sum, when, lit, current_date, broadcast, coalesce
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DecimalType, DateType, LongType, DoubleType
from delta.tables import DeltaTable
import os
from typing import Tuple, Optional

# Configure logging with enhanced formatting
logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(name)s - %(levelname)s - [%(funcName)s:%(lineno)d] - %(message)s'
)
logger = logging.getLogger(__name__)

class RegulatoryReportingETLEnhanced:
    """
    Enhanced ETL class for Regulatory Reporting with improved performance and error handling.
    """
    
    def __init__(self, app_name: str = "RegulatoryReportingETL_Enhanced_v2"):
        self.spark = self._get_spark_session(app_name)
        self.app_name = app_name
        
    def _get_spark_session(self, app_name: str) -> SparkSession:
        """
        Initializes and returns a Spark session compatible with Spark Connect with enhanced configurations.
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
                    .config("spark.sql.adaptive.localShuffleReader.enabled", "true") \
                    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
                    .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
                    .getOrCreate()
            
            # Enhanced Spark configurations for better performance
            spark.conf.set("spark.sql.adaptive.advisoryPartitionSizeInBytes", "128MB")
            spark.conf.set("spark.sql.adaptive.maxShuffledHashJoinLocalMapThreshold", "0")
            spark.conf.set("spark.sql.join.preferSortMergeJoin", "true")
            
            logger.info(f"Enhanced Spark session created successfully for {app_name}")
            return spark
        except Exception as e:
            logger.error(f"Error creating Spark session: {e}")
            raise
    
    def create_optimized_sample_data(self) -> Tuple[DataFrame, DataFrame, DataFrame, DataFrame, DataFrame]:
        """
        Creates optimized sample DataFrames with proper partitioning and caching strategies.
        """
        logger.info("Creating optimized sample data with enhanced schema validation")
        
        try:
            # Customer sample data with enhanced validation
            customer_schema = StructType([
                StructField("CUSTOMER_ID", IntegerType(), False),  # Not nullable for key field
                StructField("NAME", StringType(), False),
                StructField("EMAIL", StringType(), True),
                StructField("PHONE", StringType(), True),
                StructField("ADDRESS", StringType(), True),
                StructField("CREATED_DATE", DateType(), True)
            ])
            
            customer_data = [
                (1, "John Doe", "john.doe@email.com", "123-456-7890", "123 Main St", "2023-01-15"),
                (2, "Jane Smith", "jane.smith@email.com", "098-765-4321", "456 Oak Ave", "2023-02-20"),
                (3, "Bob Johnson", "bob.johnson@email.com", "555-123-4567", "789 Pine Rd", "2023-03-10"),
                (4, "Alice Brown", "alice.brown@email.com", "777-888-9999", "321 Elm St", "2023-04-05"),
                (5, "Charlie Wilson", "charlie.wilson@email.com", "666-555-4444", "654 Maple Ave", "2023-05-12")
            ]
            
            customer_df = self.spark.createDataFrame(customer_data, customer_schema)
            customer_df = customer_df.repartition(2, "CUSTOMER_ID").cache()  # Optimized partitioning
            
            # Branch sample data with enhanced structure
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
                (104, "West Branch", "WS004", "Seattle", "WA", "USA")
            ]
            
            branch_df = self.spark.createDataFrame(branch_data, branch_schema)
            branch_df = branch_df.cache()  # Small table, cache for broadcast joins
            
            # Account sample data with better distribution
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
                (1001, 1, 101, "ACC001001", "SAVINGS", 15000.50, "2023-01-20"),
                (1002, 2, 102, "ACC002002", "CHECKING", 8500.75, "2023-02-25"),
                (1003, 3, 103, "ACC003003", "SAVINGS", 22000.00, "2023-03-15"),
                (1004, 1, 101, "ACC001004", "CHECKING", 5500.25, "2023-04-10"),
                (1005, 4, 104, "ACC004005", "SAVINGS", 12000.00, "2023-04-15"),
                (1006, 5, 102, "ACC005006", "CHECKING", 7500.50, "2023-05-20")
            ]
            
            account_df = self.spark.createDataFrame(account_data, account_schema)
            account_df = account_df.repartition(2, "BRANCH_ID").cache()
            
            # Transaction sample data with larger volume for testing
            transaction_schema = StructType([
                StructField("TRANSACTION_ID", IntegerType(), False),
                StructField("ACCOUNT_ID", IntegerType(), False),
                StructField("TRANSACTION_TYPE", StringType(), False),
                StructField("AMOUNT", DecimalType(15,2), False),
                StructField("TRANSACTION_DATE", DateType(), True),
                StructField("DESCRIPTION", StringType(), True)
            ])
            
            transaction_data = [
                (10001, 1001, "DEPOSIT", 1000.00, "2023-05-01", "Salary deposit"),
                (10002, 1001, "WITHDRAWAL", 200.00, "2023-05-02", "ATM withdrawal"),
                (10003, 1002, "DEPOSIT", 1500.00, "2023-05-03", "Check deposit"),
                (10004, 1003, "TRANSFER", 500.00, "2023-05-04", "Online transfer"),
                (10005, 1004, "DEPOSIT", 750.00, "2023-05-05", "Cash deposit"),
                (10006, 1002, "WITHDRAWAL", 100.00, "2023-05-06", "Debit card purchase"),
                (10007, 1005, "DEPOSIT", 2000.00, "2023-05-07", "Investment return"),
                (10008, 1006, "TRANSFER", 300.00, "2023-05-08", "Bill payment"),
                (10009, 1001, "DEPOSIT", 800.00, "2023-05-09", "Bonus payment"),
                (10010, 1003, "WITHDRAWAL", 150.00, "2023-05-10", "Cash withdrawal")
            ]
            
            transaction_df = self.spark.createDataFrame(transaction_data, transaction_schema)
            transaction_df = transaction_df.repartition(4, "ACCOUNT_ID")  # More partitions for larger data
            
            # Enhanced Branch Operational Details with comprehensive data
            branch_operational_schema = StructType([
                StructField("BRANCH_ID", IntegerType(), False),
                StructField("REGION", StringType(), False),
                StructField("MANAGER_NAME", StringType(), True),
                StructField("LAST_AUDIT_DATE", DateType(), True),
                StructField("IS_ACTIVE", StringType(), False)
            ])
            
            branch_operational_data = [
                (101, "Northeast", "Alice Johnson", "2023-04-15", "Y"),
                (102, "West Coast", "Bob Wilson", "2023-03-20", "Y"),
                (103, "Midwest", "Carol Davis", "2023-02-10", "N"),  # Inactive for testing
                (104, "Northwest", "David Miller", "2023-04-25", "Y")
            ]
            
            branch_operational_df = self.spark.createDataFrame(branch_operational_data, branch_operational_schema)
            branch_operational_df = branch_operational_df.cache()  # Small lookup table
            
            logger.info("Optimized sample data created successfully with enhanced partitioning")
            return customer_df, account_df, transaction_df, branch_df, branch_operational_df
            
        except Exception as e:
            logger.error(f"Error creating sample data: {e}")
            raise
    
    def validate_data_integrity(self, df: DataFrame, table_name: str, key_columns: list) -> bool:
        """
        Enhanced data integrity validation with comprehensive checks.
        """
        logger.info(f"Performing comprehensive data integrity validation for {table_name}")
        
        try:
            # Check for empty DataFrame
            row_count = df.count()
            if row_count == 0:
                logger.warning(f"{table_name} is empty")
                return False
            
            # Check for null values in key columns
            for key_col in key_columns:
                null_count = df.filter(col(key_col).isNull()).count()
                if null_count > 0:
                    logger.error(f"{table_name} has {null_count} null values in key column {key_col}")
                    return False
            
            # Check for duplicate keys if applicable
            if len(key_columns) == 1:
                distinct_count = df.select(key_columns[0]).distinct().count()
                if distinct_count != row_count:
                    logger.warning(f"{table_name} has duplicate values in key column {key_columns[0]}")
            
            # Data type validation for numeric columns
            numeric_columns = [field.name for field in df.schema.fields 
                             if isinstance(field.dataType, (IntegerType, LongType, DoubleType, DecimalType))]
            
            for num_col in numeric_columns:
                negative_count = df.filter(col(num_col) < 0).count()
                if negative_count > 0 and num_col in ["TOTAL_TRANSACTIONS", "TOTAL_AMOUNT"]:
                    logger.error(f"{table_name} has {negative_count} negative values in {num_col}")
                    return False
            
            logger.info(f"Data integrity validation passed for {table_name}. Row count: {row_count}")
            return True
            
        except Exception as e:
            logger.error(f"Data integrity validation failed for {table_name}: {e}")
            return False
    
    def create_aml_customer_transactions_optimized(self, customer_df: DataFrame, account_df: DataFrame, 
                                                  transaction_df: DataFrame) -> DataFrame:
        """
        Creates optimized AML_CUSTOMER_TRANSACTIONS DataFrame with broadcast joins.
        """
        logger.info("Creating optimized AML Customer Transactions DataFrame")
        
        try:
            # Use broadcast join for smaller tables to optimize performance
            result_df = transaction_df \
                .join(broadcast(account_df), "ACCOUNT_ID") \
                .join(broadcast(customer_df), "CUSTOMER_ID") \
                .select(
                    col("CUSTOMER_ID"),
                    col("NAME"),
                    col("ACCOUNT_ID"),
                    col("TRANSACTION_ID"),
                    col("AMOUNT"),
                    col("TRANSACTION_TYPE"),
                    col("TRANSACTION_DATE")
                ) \
                .orderBy("CUSTOMER_ID", "TRANSACTION_DATE")
            
            logger.info("AML Customer Transactions DataFrame created with broadcast optimization")
            return result_df
            
        except Exception as e:
            logger.error(f"Error creating AML Customer Transactions: {e}")
            raise
    
    def create_enhanced_branch_summary_report_v2(self, transaction_df: DataFrame, account_df: DataFrame, 
                                                branch_df: DataFrame, branch_operational_df: DataFrame) -> DataFrame:
        """
        Creates the enhanced BRANCH_SUMMARY_REPORT DataFrame with performance optimizations and better error handling.
        
        Version 2 Enhancements:
        - Improved join strategies with broadcast hints
        - Enhanced null handling with coalesce functions
        - Better data type casting and validation
        - Optimized aggregation logic
        """
        logger.info("Creating Enhanced Branch Summary Report v2 with performance optimizations")
        
        try:
            # Step 1: Create base branch summary with optimized joins and aggregations
            logger.info("Step 1: Creating base branch summary with transaction aggregations")
            
            base_summary = transaction_df \
                .join(account_df, "ACCOUNT_ID") \
                .join(broadcast(branch_df), "BRANCH_ID") \
                .groupBy("BRANCH_ID", "BRANCH_NAME") \
                .agg(
                    count("*").alias("TOTAL_TRANSACTIONS"),
                    spark_sum("AMOUNT").alias("TOTAL_AMOUNT")
                ) \
                .filter(col("TOTAL_TRANSACTIONS") > 0)  # Filter out branches with no transactions
            
            logger.info(f"Base summary created with {base_summary.count()} branches")
            
            # Step 2: Enhanced join with BRANCH_OPERATIONAL_DETAILS with better null handling
            logger.info("Step 2: Joining with BRANCH_OPERATIONAL_DETAILS and applying conditional logic")
            
            enhanced_summary = base_summary.join(
                broadcast(branch_operational_df), 
                base_summary["BRANCH_ID"] == branch_operational_df["BRANCH_ID"], 
                "left"  # Left join to maintain all branches
            ).select(
                base_summary["BRANCH_ID"].cast(LongType()).alias("BRANCH_ID"),
                base_summary["BRANCH_NAME"],
                base_summary["TOTAL_TRANSACTIONS"],
                base_summary["TOTAL_AMOUNT"].cast(DoubleType()).alias("TOTAL_AMOUNT"),
                # Enhanced conditional population with better null handling
                when(coalesce(branch_operational_df["IS_ACTIVE"], lit("N")) == "Y", 
                     branch_operational_df["REGION"]).alias("REGION"),
                when(coalesce(branch_operational_df["IS_ACTIVE"], lit("N")) == "Y", 
                     branch_operational_df["LAST_AUDIT_DATE"].cast(StringType())).alias("LAST_AUDIT_DATE")
            ).orderBy("BRANCH_ID")
            
            # Step 3: Data quality validation
            logger.info("Step 3: Performing data quality validation")
            
            if not self.validate_data_integrity(enhanced_summary, "BRANCH_SUMMARY_REPORT", ["BRANCH_ID"]):
                raise ValueError("Data integrity validation failed for BRANCH_SUMMARY_REPORT")
            
            logger.info("Enhanced Branch Summary Report v2 created successfully with all validations passed")
            return enhanced_summary
            
        except Exception as e:
            logger.error(f"Error creating Enhanced Branch Summary Report v2: {e}")
            raise
    
    def write_to_delta_table_enhanced(self, df: DataFrame, table_path: str, mode: str = "overwrite", 
                                    partition_by: Optional[list] = None) -> bool:
        """
        Enhanced Delta table writing with better error handling and optimization options.
        """
        logger.info(f"Writing DataFrame to Delta table: {table_path} with mode: {mode}")
        
        try:
            writer = df.write.format("delta") \
                      .mode(mode) \
                      .option("mergeSchema", "true") \
                      .option("optimizeWrite", "true") \
                      .option("autoCompact", "true")
            
            if partition_by:
                writer = writer.partitionBy(*partition_by)
                logger.info(f"Partitioning by: {partition_by}")
            
            writer.save(table_path)
            
            # Verify write success
            verification_df = self.spark.read.format("delta").load(table_path)
            written_count = verification_df.count()
            
            logger.info(f"Successfully written {written_count} records to {table_path}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to write to Delta table {table_path}: {e}")
            return False
    
    def execute_etl_pipeline(self) -> bool:
        """
        Main ETL execution function with enhanced error handling and monitoring.
        """
        logger.info(f"Starting Enhanced ETL Pipeline execution - {self.app_name}")
        
        try:
            # Step 1: Create optimized sample data
            logger.info("=== Step 1: Creating optimized sample data ===")
            customer_df, account_df, transaction_df, branch_df, branch_operational_df = self.create_optimized_sample_data()
            
            # Step 2: Display sample operational data for verification
            logger.info("=== Step 2: Sample BRANCH_OPERATIONAL_DETAILS verification ===")
            logger.info("BRANCH_OPERATIONAL_DETAILS sample:")
            branch_operational_df.show(truncate=False)
            
            # Step 3: Create and validate AML Customer Transactions
            logger.info("=== Step 3: Processing AML Customer Transactions ===")
            aml_transactions_df = self.create_aml_customer_transactions_optimized(
                customer_df, account_df, transaction_df
            )
            
            if self.validate_data_integrity(aml_transactions_df, "AML_CUSTOMER_TRANSACTIONS", ["TRANSACTION_ID"]):
                logger.info("AML_CUSTOMER_TRANSACTIONS processing completed successfully")
                # In production: self.write_to_delta_table_enhanced(aml_transactions_df, "/path/to/aml_transactions")
            else:
                logger.error("AML_CUSTOMER_TRANSACTIONS validation failed")
                return False
            
            # Step 4: Create and validate Enhanced Branch Summary Report
            logger.info("=== Step 4: Processing Enhanced Branch Summary Report v2 ===")
            enhanced_branch_summary_df = self.create_enhanced_branch_summary_report_v2(
                transaction_df, account_df, branch_df, branch_operational_df
            )
            
            # Display results for verification
            logger.info("Enhanced BRANCH_SUMMARY_REPORT v2 Results:")
            enhanced_branch_summary_df.show(truncate=False)
            
            if self.validate_data_integrity(enhanced_branch_summary_df, "BRANCH_SUMMARY_REPORT", ["BRANCH_ID"]):
                logger.info("Enhanced BRANCH_SUMMARY_REPORT v2 processing completed successfully")
                # In production: self.write_to_delta_table_enhanced(enhanced_branch_summary_df, "/path/to/branch_summary")
            else:
                logger.error("Enhanced BRANCH_SUMMARY_REPORT v2 validation failed")
                return False
            
            # Step 5: Performance metrics
            logger.info("=== Step 5: Performance Summary ===")
            logger.info(f"Total customers processed: {customer_df.count()}")
            logger.info(f"Total transactions processed: {transaction_df.count()}")
            logger.info(f"Total branches in summary: {enhanced_branch_summary_df.count()}")
            logger.info(f"Active branches with operational details: {enhanced_branch_summary_df.filter(col('REGION').isNotNull()).count()}")
            
            logger.info("Enhanced ETL Pipeline execution completed successfully")
            return True
            
        except Exception as e:
            logger.error(f"Enhanced ETL Pipeline execution failed: {e}")
            return False
        finally:
            # Cleanup cached DataFrames to free memory
            try:
                self.spark.catalog.clearCache()
                logger.info("Cache cleared successfully")
            except Exception as cleanup_error:
                logger.warning(f"Cache cleanup warning: {cleanup_error}")

def main():
    """
    Main execution function for Enhanced Regulatory Reporting ETL v2.
    """
    etl_processor = None
    
    try:
        logger.info("Initializing Enhanced Regulatory Reporting ETL v2")
        etl_processor = RegulatoryReportingETLEnhanced()
        
        success = etl_processor.execute_etl_pipeline()
        
        if success:
            logger.info("ETL Pipeline completed successfully!")
            return 0
        else:
            logger.error("ETL Pipeline failed!")
            return 1
            
    except Exception as e:
        logger.error(f"Main execution failed: {e}")
        return 1
    finally:
        if etl_processor and etl_processor.spark:
            # Don't stop the session in Databricks environment
            logger.info("ETL execution completed - session maintained for Databricks compatibility")

if __name__ == "__main__":
    exit_code = main()
    # In Databricks, we don't typically exit with sys.exit()
    if exit_code != 0:
        logger.error(f"Pipeline failed with exit code: {exit_code}")