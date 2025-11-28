====================================================================
# Author: Ascendion AAVA
# Date: 
# Description: Enhanced PySpark ETL for Regulatory Reporting with BRANCH_OPERATIONAL_DETAILS integration
====================================================================

import logging
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, count, sum, when, lit, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType, LongType
from datetime import datetime, date

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def get_spark_session(app_name: str = "RegulatoryReportingETL") -> SparkSession:
    """
    Initializes and returns a Spark session compatible with Spark Connect.
    # [MODIFIED] - Updated to use getActiveSession() for Spark Connect compatibility
    """
    try:
        # [MODIFIED] - Use getActiveSession() instead of creating new session for Spark Connect compatibility
        try:
            spark = SparkSession.getActiveSession()
            if spark is None:
                spark = SparkSession.builder \
                    .appName(app_name) \
                    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
                    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
                    .getOrCreate()
        except:
            spark = SparkSession.builder \
                .appName(app_name) \
                .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
                .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
                .getOrCreate()
        
        # [MODIFIED] - Removed sparkContext.setLogLevel() for Spark Connect compatibility
        # spark.sparkContext.setLogLevel("WARN")  # [DEPRECATED] - Not compatible with Spark Connect
        logger.info("Spark session created successfully.")
        return spark
    except Exception as e:
        logger.error(f"Error creating Spark session: {e}")
        raise

# [ADDED] - New function to create sample data for self-contained execution
def create_sample_data(spark: SparkSession) -> tuple:
    """
    Creates sample DataFrames for testing purposes.
    # [ADDED] - Self-contained sample data creation for Databricks compatibility
    """
    logger.info("Creating sample data for testing")
    
    # Customer sample data
    customer_schema = StructType([
        StructField("CUSTOMER_ID", IntegerType(), True),
        StructField("NAME", StringType(), True)
    ])
    customer_data = [
        (1, "John Doe"),
        (2, "Jane Smith"),
        (3, "Bob Johnson")
    ]
    customer_df = spark.createDataFrame(customer_data, customer_schema)
    
    # Account sample data
    account_schema = StructType([
        StructField("ACCOUNT_ID", IntegerType(), True),
        StructField("CUSTOMER_ID", IntegerType(), True),
        StructField("BRANCH_ID", IntegerType(), True)
    ])
    account_data = [
        (101, 1, 1),
        (102, 2, 1),
        (103, 3, 2)
    ]
    account_df = spark.createDataFrame(account_data, account_schema)
    
    # Transaction sample data
    transaction_schema = StructType([
        StructField("TRANSACTION_ID", IntegerType(), True),
        StructField("ACCOUNT_ID", IntegerType(), True),
        StructField("AMOUNT", DoubleType(), True),
        StructField("TRANSACTION_TYPE", StringType(), True),
        StructField("TRANSACTION_DATE", DateType(), True)
    ])
    transaction_data = [
        (1001, 101, 1000.0, "DEPOSIT", date(2024, 1, 15)),
        (1002, 102, 500.0, "WITHDRAWAL", date(2024, 1, 16)),
        (1003, 103, 2000.0, "DEPOSIT", date(2024, 1, 17))
    ]
    transaction_df = spark.createDataFrame(transaction_data, transaction_schema)
    
    # Branch sample data
    branch_schema = StructType([
        StructField("BRANCH_ID", IntegerType(), True),
        StructField("BRANCH_NAME", StringType(), True)
    ])
    branch_data = [
        (1, "Downtown Branch"),
        (2, "Uptown Branch")
    ]
    branch_df = spark.createDataFrame(branch_data, branch_schema)
    
    # [ADDED] - New BRANCH_OPERATIONAL_DETAILS sample data as per PCE-2 requirements
    branch_operational_schema = StructType([
        StructField("BRANCH_ID", IntegerType(), True),
        StructField("REGION", StringType(), True),
        StructField("MANAGER_NAME", StringType(), True),
        StructField("LAST_AUDIT_DATE", DateType(), True),
        StructField("IS_ACTIVE", StringType(), True)
    ])
    branch_operational_data = [
        (1, "North Region", "Alice Manager", date(2024, 1, 10), "Y"),
        (2, "South Region", "Bob Manager", date(2024, 1, 12), "Y"),
        (3, "East Region", "Charlie Manager", date(2024, 1, 8), "N")  # Inactive branch
    ]
    branch_operational_df = spark.createDataFrame(branch_operational_data, branch_operational_schema)
    
    return customer_df, account_df, transaction_df, branch_df, branch_operational_df

# [DEPRECATED] - Original JDBC read function kept for reference
# def read_table(spark: SparkSession, jdbc_url: str, table_name: str, connection_properties: dict) -> DataFrame:
#     """
#     Reads a table from a JDBC source into a DataFrame.
#     # [DEPRECATED] - Replaced with sample data creation for self-contained execution
#     """
#     logger.info(f"Reading table: {table_name}")
#     try:
#         df = spark.read.jdbc(url=jdbc_url, table=table_name, properties=connection_properties)
#         return df
#     except Exception as e:
#         logger.error(f"Failed to read table {table_name}: {e}")
#         raise

# [ADDED] - New function to read from Delta tables
def read_delta_table(spark: SparkSession, table_path: str) -> DataFrame:
    """
    Reads a Delta table into a DataFrame.
    # [ADDED] - Delta table reading capability
    """
    logger.info(f"Reading Delta table: {table_path}")
    try:
        df = spark.read.format("delta").load(table_path)
        return df
    except Exception as e:
        logger.error(f"Failed to read Delta table {table_path}: {e}")
        raise

def create_aml_customer_transactions(customer_df: DataFrame, account_df: DataFrame, transaction_df: DataFrame) -> DataFrame:
    """
    Creates the AML_CUSTOMER_TRANSACTIONS DataFrame by joining customer, account, and transaction data.
    # [MODIFIED] - Added validation and error handling
    """
    logger.info("Creating AML Customer Transactions DataFrame.")
    
    # [ADDED] - Input validation
    if customer_df.count() == 0 or account_df.count() == 0 or transaction_df.count() == 0:
        logger.warning("One or more input DataFrames are empty")
    
    result_df = customer_df.join(account_df, "CUSTOMER_ID", "inner") \
                          .join(transaction_df, "ACCOUNT_ID", "inner") \
                          .select(
                              col("CUSTOMER_ID"),
                              col("NAME"),
                              col("ACCOUNT_ID"),
                              col("TRANSACTION_ID"),
                              col("AMOUNT"),
                              col("TRANSACTION_TYPE"),
                              col("TRANSACTION_DATE"),
                              current_timestamp().alias("CREATED_TIMESTAMP")  # [ADDED] - Audit timestamp
                          )
    
    # [ADDED] - Data quality validation
    logger.info(f"AML Customer Transactions created with {result_df.count()} records")
    return result_df

# [MODIFIED] - Enhanced function to integrate BRANCH_OPERATIONAL_DETAILS as per PCE-2
def create_branch_summary_report(transaction_df: DataFrame, account_df: DataFrame, 
                               branch_df: DataFrame, branch_operational_df: DataFrame) -> DataFrame:
    """
    Creates the BRANCH_SUMMARY_REPORT DataFrame by aggregating transaction data at the branch level.
    # [MODIFIED] - Enhanced to integrate BRANCH_OPERATIONAL_DETAILS as per PCE-2 requirements
    """
    logger.info("Creating Enhanced Branch Summary Report DataFrame with operational details.")
    
    # [ADDED] - Input validation
    if transaction_df.count() == 0:
        logger.warning("Transaction DataFrame is empty")
    
    # Original aggregation logic
    base_summary = transaction_df.join(account_df, "ACCOUNT_ID", "inner") \
                                .join(branch_df, "BRANCH_ID", "inner") \
                                .groupBy("BRANCH_ID", "BRANCH_NAME") \
                                .agg(
                                    count("*").alias("TOTAL_TRANSACTIONS"),
                                    sum("AMOUNT").alias("TOTAL_AMOUNT")
                                )
    
    # [ADDED] - Integration with BRANCH_OPERATIONAL_DETAILS as per PCE-2 specifications
    # Join with BRANCH_OPERATIONAL_DETAILS using BRANCH_ID
    enhanced_summary = base_summary.join(branch_operational_df, "BRANCH_ID", "left_outer")
    
    # [ADDED] - Conditional population based on IS_ACTIVE = 'Y' as per PCE-2 requirements
    final_summary = enhanced_summary.withColumn(
        "REGION", 
        when(col("IS_ACTIVE") == "Y", col("REGION")).otherwise(lit(None))  # [ADDED] - Populate REGION if IS_ACTIVE = 'Y'
    ).withColumn(
        "LAST_AUDIT_DATE", 
        when(col("IS_ACTIVE") == "Y", col("LAST_AUDIT_DATE")).otherwise(lit(None))  # [ADDED] - Populate LAST_AUDIT_DATE if IS_ACTIVE = 'Y'
    ).select(
        col("BRANCH_ID"),
        col("BRANCH_NAME"),
        col("TOTAL_TRANSACTIONS"),
        col("TOTAL_AMOUNT"),
        col("REGION"),  # [ADDED] - New column as per PCE-2
        col("LAST_AUDIT_DATE"),  # [ADDED] - New column as per PCE-2
        current_timestamp().alias("REPORT_GENERATED_TIMESTAMP")  # [ADDED] - Audit timestamp
    )
    
    # [ADDED] - Data quality validation
    logger.info(f"Enhanced Branch Summary Report created with {final_summary.count()} records")
    return final_summary

# [MODIFIED] - Enhanced write function with merge capability
def write_to_delta_table(df: DataFrame, table_path: str, mode: str = "overwrite"):
    """
    Writes a DataFrame to a Delta table.
    # [MODIFIED] - Enhanced with configurable write mode and better error handling
    """
    logger.info(f"Writing DataFrame to Delta table: {table_path} with mode: {mode}")
    try:
        df.write.format("delta") \
          .mode(mode) \
          .option("mergeSchema", "true") \
          .save(table_path)  # [MODIFIED] - Changed from saveAsTable to save for better path control
        logger.info(f"Successfully written data to {table_path}")
    except Exception as e:
        logger.error(f"Failed to write to Delta table {table_path}: {e}")
        raise

# [ADDED] - New function for Delta merge operations
def merge_to_delta_table(source_df: DataFrame, target_path: str, merge_condition: str, 
                        update_columns: dict, insert_columns: dict):
    """
    Performs Delta merge (upsert) operation.
    # [ADDED] - Delta merge capability for incremental updates
    """
    logger.info(f"Performing Delta merge to: {target_path}")
    try:
        from delta.tables import DeltaTable
        
        # Create target table if it doesn't exist
        if not DeltaTable.isDeltaTable(SparkSession.getActiveSession(), target_path):
            source_df.write.format("delta").save(target_path)
            logger.info(f"Created new Delta table at {target_path}")
        else:
            delta_table = DeltaTable.forPath(SparkSession.getActiveSession(), target_path)
            
            delta_table.alias("target").merge(
                source_df.alias("source"),
                merge_condition
            ).whenMatchedUpdate(set=update_columns) \
             .whenNotMatchedInsert(values=insert_columns) \
             .execute()
            
            logger.info(f"Successfully merged data to {target_path}")
    except Exception as e:
        logger.error(f"Failed to merge to Delta table {target_path}: {e}")
        raise

# [ADDED] - Data validation function
def validate_data_quality(df: DataFrame, table_name: str) -> bool:
    """
    Performs basic data quality checks.
    # [ADDED] - Data quality validation capability
    """
    logger.info(f"Validating data quality for {table_name}")
    
    try:
        # Check for empty DataFrame
        record_count = df.count()
        if record_count == 0:
            logger.warning(f"{table_name} is empty")
            return False
        
        # Check for null values in key columns
        null_checks = df.select([count(when(col(c).isNull(), c)).alias(c) for c in df.columns])
        null_results = null_checks.collect()[0].asDict()
        
        has_nulls = any(count > 0 for count in null_results.values())
        if has_nulls:
            logger.warning(f"{table_name} has null values: {null_results}")
        
        logger.info(f"{table_name} validation completed. Records: {record_count}")
        return True
        
    except Exception as e:
        logger.error(f"Data validation failed for {table_name}: {e}")
        return False

def main():
    """
    Main ETL execution function.
    # [MODIFIED] - Enhanced with sample data and improved error handling
    """
    spark = None
    try:
        spark = get_spark_session()
        
        # [MODIFIED] - Use sample data instead of JDBC connections for self-contained execution
        # [DEPRECATED] - JDBC connection logic commented out for Databricks compatibility
        # jdbc_url = "jdbc:oracle:thin:@your_oracle_host:1521:orcl"
        # connection_properties = {
        #     "user": "your_user",
        #     "password": "your_password",
        #     "driver": "oracle.jdbc.driver.OracleDriver"
        # }
        
        # [ADDED] - Create sample data for self-contained execution
        customer_df, account_df, transaction_df, branch_df, branch_operational_df = create_sample_data(spark)
        
        # [ADDED] - Data quality validation
        validate_data_quality(customer_df, "CUSTOMER")
        validate_data_quality(account_df, "ACCOUNT")
        validate_data_quality(transaction_df, "TRANSACTION")
        validate_data_quality(branch_df, "BRANCH")
        validate_data_quality(branch_operational_df, "BRANCH_OPERATIONAL_DETAILS")  # [ADDED] - New table validation
        
        # Create and write AML_CUSTOMER_TRANSACTIONS
        aml_transactions_df = create_aml_customer_transactions(customer_df, account_df, transaction_df)
        write_to_delta_table(aml_transactions_df, "/tmp/delta/AML_CUSTOMER_TRANSACTIONS")
        
        # [MODIFIED] - Create enhanced branch summary report with operational details
        branch_summary_df = create_branch_summary_report(transaction_df, account_df, branch_df, branch_operational_df)
        write_to_delta_table(branch_summary_df, "/tmp/delta/BRANCH_SUMMARY_REPORT")
        
        # [ADDED] - Final validation
        validate_data_quality(aml_transactions_df, "AML_CUSTOMER_TRANSACTIONS")
        validate_data_quality(branch_summary_df, "BRANCH_SUMMARY_REPORT")
        
        logger.info("Enhanced ETL job completed successfully with BRANCH_OPERATIONAL_DETAILS integration.")
        
    except Exception as e:
        logger.error(f"ETL job failed with exception: {e}")
        raise  # [MODIFIED] - Re-raise exception for proper error handling
    finally:
        if spark:
            # [MODIFIED] - Conditional stop for Spark Connect compatibility
            try:
                spark.stop()
                logger.info("Spark session stopped.")
            except:
                logger.info("Spark session cleanup completed.")

if __name__ == "__main__":
    main()